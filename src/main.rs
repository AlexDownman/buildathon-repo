//! Shopping Sessions pipeline. See rules.md.
//!
//! Phases:
//!  1. products, campaigns (tiny), links -> union-find -> map ID key -> person name key
//!  2. parse events + clicks in parallel chunks. Events go to buckets by
//!     hash(event_id) for dedup; valid clicks go straight to buckets by hash(person).
//!  3. dedup each event bucket (smallest ingest_ts wins), survivors go to person buckets
//!  4. per person bucket: sort, sessionize, credit, assists, aggregate
//!  5. write Parquet

mod alloc;
mod output;
mod part;
mod util;

#[global_allocator]
static GLOBAL: alloc::BigAlloc = alloc::BigAlloc;

use parquet::data_type::ByteArray;
use part::Parts;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering as AO};
use std::time::Instant;
use util::*;

const EV_BITS: u32 = 9; // dedup buckets
const P_BITS: u32 = 10; // person buckets
const CHUNK: usize = 8 << 20;
const PAD: usize = 256;

const K_INVALID: u8 = 0;
const K_VIEW: u8 = 1;
const K_CART: u8 = 2;
const K_PURCHASE: u8 = 3;

#[derive(Clone, Copy, Default)]
#[repr(C, align(64))]
struct EvRec {
    ekey: u128,
    pkey: u128,
    ts: i64,
    ingest: i64,
    price: u32,
    cat: u16,
    kind: u8,
}

#[derive(Clone, Copy, Default)]
struct EvOut {
    pkey: u128,
    ts: i64,
    price: u32,
    cat: u16,
    kind: u8,
}

#[derive(Clone, Copy, Default)]
struct ClRec {
    pkey: u128,
    ckey: u128,
    ts: i64,
    camp: u32,
}

struct URow {
    pkey: u128,
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    top: u16, // u16::MAX = null
}

struct Ctx {
    ws: i64,
    we: i64,
    link: KeyMap<u128>,
    products: KeyMap<u16>,
    unknown_cat: u16,
    cat_names: Vec<Vec<u8>>,
    camps: KeyMap<u32>,
    camp_ids: Vec<Vec<u8>>,
    channels: Vec<Vec<u8>>,
    day0: i64,
    ndays: usize,
}

impl Ctx {
    fn ncamp(&self) -> usize {
        self.camp_ids.len()
    }
}

fn log(t0: Instant, msg: &str) {
    eprintln!("[{:8.3}s] {}", t0.elapsed().as_secs_f64(), msg);
}

/// Runs f(state, task) for task in 0..ntasks on nthreads workers; returns the states.
fn par_for<T: Send, I: Fn() -> T + Sync, F: Fn(&mut T, usize) + Sync>(nthreads: usize, ntasks: usize, init: I, f: F) -> Vec<T> {
    let next = AtomicUsize::new(0);
    std::thread::scope(|s| {
        let hs: Vec<_> = (0..nthreads)
            .map(|_| {
                s.spawn(|| {
                    let mut st = init();
                    loop {
                        let i = next.fetch_add(1, AO::Relaxed);
                        if i >= ntasks {
                            break;
                        }
                        f(&mut st, i);
                    }
                    st
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

// ---------------------------------------------------------------------------
// Input files, read in chunks with pread into padded buffers
// ---------------------------------------------------------------------------

struct Input {
    files: Vec<(File, usize)>,
}

impl Input {
    fn open(dir: &Path) -> Input {
        let mut paths = Vec::new();
        if let Ok(rd) = std::fs::read_dir(dir) {
            for e in rd.flatten() {
                let p = e.path();
                if p.extension().map(|x| x == "csv").unwrap_or(false) && p.is_file() {
                    paths.push(p);
                }
            }
        }
        paths.sort();
        let mut files = Vec::new();
        for p in paths {
            if let Ok(f) = File::open(&p) {
                let len = f.metadata().map(|m| m.len() as usize).unwrap_or(0);
                if len > 0 {
                    files.push((f, len));
                }
            }
        }
        Input { files }
    }

    fn bytes(&self) -> usize {
        self.files.iter().map(|f| f.1).sum()
    }

    fn chunks(&self) -> Vec<(usize, usize, usize)> {
        let mut v = Vec::new();
        for (i, (_, n)) in self.files.iter().enumerate() {
            let mut s = 0;
            while s < *n {
                let e = (s + CHUNK).min(*n);
                v.push((i, s, e));
                s = e;
            }
        }
        v
    }

    /// Reads chunk (fi, s, e) into buf and returns (s', e') relative to it.
    /// buf holds every line starting in [s, e), each ending in '\n', plus
    /// at least PAD zero bytes of readable padding after the logical data.
    fn read(&self, fi: usize, s: usize, e: usize, buf: &mut Vec<u8>) -> (usize, usize) {
        let (f, flen) = (&self.files[fi].0, self.files[fi].1);
        let off = s.saturating_sub(1);
        let mut extra = 64 << 10;
        loop {
            let end = (e + extra).min(flen);
            let n = end - off;
            if buf.len() < n + 1 + PAD {
                buf.resize(n + 1 + PAD, 0);
            }
            f.read_exact_at(&mut buf[..n], off as u64).expect("read");
            let le = e - off;
            if end == flen {
                if buf[n - 1] != b'\n' {
                    buf[n] = b'\n';
                    buf[n + 1..n + 1 + PAD].fill(0);
                } else {
                    buf[n..n + PAD].fill(0);
                }
                return (s - off, le);
            }
            if memchr::memrchr(b'\n', &buf[le - 1..n]).is_some() {
                buf[n..n + PAD].fill(0);
                return (s - off, le);
            }
            extra *= 4;
        }
    }
}

fn manifest_value(text: &str, name: &str) -> i64 {
    let i = text.find(&format!("\"{}\"", name)).expect("manifest key");
    let rest = &text[i + name.len() + 2..];
    let a = rest.find('"').unwrap();
    let rest = &rest[a + 1..];
    let b = rest.find('"').unwrap();
    parse_ts(rest[..b].as_bytes()).expect("manifest timestamp")
}

fn read_small(p: &Path) -> Vec<u8> {
    std::fs::read(p).unwrap_or_default()
}

fn main() {
    let t0 = Instant::now();
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let nthreads: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8))
        .clamp(1, 31);

    let manifest = std::fs::read_to_string(data.join("manifest.json")).expect("manifest.json");
    let ws = manifest_value(&manifest, "window_start");
    let we = manifest_value(&manifest, "window_end");
    let day0 = ws.div_euclid(DAY_MS);
    let ndays = ((we - 1).div_euclid(DAY_MS) - day0 + 1).max(1) as usize;

    // ---- products ----
    let pbytes = read_small(&data.join("products.csv"));
    let mut prows: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for_lines(&pbytes, 0, pbytes.len(), |line| {
        let mut f: [&[u8]; 2] = [b""; 2];
        if split(line, &mut f) && !f[0].is_empty() {
            prows.push((f[0].to_vec(), f[1].to_vec()));
        }
    });
    let mut cat_names: Vec<Vec<u8>> = prows.iter().map(|r| r.1.clone()).collect();
    cat_names.push(b"unknown".to_vec());
    cat_names.sort();
    cat_names.dedup();
    let cat_rank = |n: &[u8]| cat_names.binary_search_by(|x| x.as_slice().cmp(n)).unwrap() as u16;
    let unknown_cat = cat_rank(b"unknown");
    let mut products = KeyMap::<u16>::with_capacity(prows.len());
    for (id, c) in &prows {
        products.get_or_insert(key_of(id), cat_rank(c));
    }

    // ---- campaigns ----
    let cbytes = read_small(&data.join("campaigns.csv"));
    let mut camps = KeyMap::<u32>::with_capacity(4096);
    let mut camp_ids = Vec::new();
    let mut channels = Vec::new();
    for_lines(&cbytes, 0, cbytes.len(), |line| {
        let mut f: [&[u8]; 2] = [b""; 2];
        if split(line, &mut f) && !f[0].is_empty() {
            let (_, new) = camps.get_or_insert(key_of(f[0]), camp_ids.len() as u32);
            if new {
                camp_ids.push(f[0].to_vec());
                channels.push(f[1].to_vec());
            }
        }
    });
    log(t0, &format!("products {} cats {} campaigns {}", prows.len(), cat_names.len(), camp_ids.len()));

    // ---- links ----
    let links_in = Input::open(&data.join("links"));
    let link_chunks = links_in.chunks();
    let pair_lists = par_for(
        nthreads,
        link_chunks.len(),
        || (Vec::<(u128, u128)>::new(), Vec::<u8>::new()),
        |(v, buf), i| {
            let (fi, s, e) = link_chunks[i];
            let (s, e) = links_in.read(fi, s, e, buf);
            let d = &buf[..];
            let end = d.as_ptr_range().end;
            for_rows::<2>(d, s, e, |f| {
                if !f[0].is_empty() && !f[1].is_empty() {
                    v.push((key_fast(f[0], end), key_fast(f[1], end)));
                }
            });
        },
    );
    let npairs: usize = pair_lists.iter().map(|v| v.0.len()).sum();
    let link = build_links(&pair_lists, npairs);
    drop(pair_lists);
    log(t0, &format!("links: {} pairs, {} linked ids", npairs, link.len()));

    let ctx = Ctx {
        ws,
        we,
        link,
        products,
        unknown_cat,
        cat_names: cat_names.clone(),
        camps,
        camp_ids,
        channels,
        day0,
        ndays,
    };

    // ---- parse events + clicks ----
    let ev_in = Input::open(&data.join("events"));
    let cl_in = Input::open(&data.join("clicks"));
    let ev_chunks = ev_in.chunks();
    let cl_chunks = cl_in.chunks();
    let ev_bytes = ev_in.bytes();
    let cl_bytes = cl_in.bytes();
    let nev_b = 1usize << EV_BITS;
    let np_b = 1usize << P_BITS;
    let ev_reserve = ev_bytes / 72 / nev_b / nthreads + 16;
    let cl_reserve = cl_bytes / 48 / np_b / nthreads + 16;
    let ntask = ev_chunks.len() + cl_chunks.len();

    struct ParseOut {
        ev: Parts<EvRec, 8>,
        cl: Parts<ClRec, 4>,
        buf: Vec<u8>,
    }
    let parsed = par_for(
        nthreads,
        ntask,
        || ParseOut { ev: Parts::new(nev_b, ev_reserve), cl: Parts::new(np_b, cl_reserve), buf: Vec::new() },
        |po, i| {
            if i < ev_chunks.len() {
                let (fi, s, e) = ev_chunks[i];
                let (s, e) = ev_in.read(fi, s, e, &mut po.buf);
                parse_events(&po.buf, s, e, &ctx, &mut po.ev);
            } else {
                let (fi, s, e) = cl_chunks[i - ev_chunks.len()];
                let (s, e) = cl_in.read(fi, s, e, &mut po.buf);
                parse_clicks(&po.buf, s, e, &ctx, &mut po.cl);
            }
        },
    );
    let mut ev_parts: Vec<Vec<Vec<EvRec>>> = Vec::with_capacity(parsed.len());
    let mut cl_parts: Vec<Vec<Vec<ClRec>>> = Vec::with_capacity(parsed.len());
    for p in parsed {
        ev_parts.push(p.ev.finish());
        cl_parts.push(p.cl.finish());
    }
    let nrec: usize = ev_parts.iter().flatten().map(|v| v.len()).sum();
    let ncl: usize = cl_parts.iter().flatten().map(|v| v.len()).sum();
    log(t0, &format!("parsed: {} event rows, {} valid clicks ({} + {} bytes)", nrec, ncl, ev_bytes, cl_bytes));

    // ---- dedup ----
    let dd_reserve = nrec / np_b / nthreads + 16;
    let deduped: Vec<Vec<Vec<EvOut>>> = par_for(
        nthreads,
        nev_b,
        || (Parts::<EvOut, 8>::new(np_b, dd_reserve), Vec::<u64>::new(), Vec::<u64>::new()),
        |(outp, table, losers), b| {
            dedup_bucket(&ev_parts, b, outp, table, losers);
        },
    )
    .into_iter()
    .map(|(p, _, _)| p.finish())
    .collect();
    drop(ev_parts);
    let nsurv: usize = deduped.iter().flatten().map(|v| v.len()).sum();
    log(t0, &format!("dedup: {} surviving events", nsurv));

    // ---- per person ----
    struct POut {
        rows: Vec<URow>,
        camp: Vec<[i64; 4]>,
        sc: Scratch,
        evs: Vec<EvOut>,
        cls: Vec<ClRec>,
    }
    let ncamp_tot = ctx.ncamp() + 1;
    let pouts = par_for(
        nthreads,
        np_b,
        || POut {
            rows: Vec::new(),
            camp: vec![[0i64; 4]; ncamp_tot * ndays],
            sc: Scratch::new(ctx.cat_names.len()),
            evs: Vec::new(),
            cls: Vec::new(),
        },
        |po, b| {
            po.evs.clear();
            for d in &deduped {
                po.evs.extend_from_slice(&d[b]);
            }
            po.cls.clear();
            for d in &cl_parts {
                po.cls.extend_from_slice(&d[b]);
            }
            process_bucket(&mut po.evs, &mut po.cls, &ctx, &mut po.sc, &mut po.rows, &mut po.camp);
        },
    );
    drop(deduped);
    drop(cl_parts);
    let nrows: usize = pouts.iter().map(|p| p.rows.len()).sum();
    log(t0, &format!("sessions done: {} user_daily rows", nrows));

    // ---- write ----
    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    std::fs::create_dir_all(&ud).unwrap();
    std::fs::create_dir_all(&cd).unwrap();

    let mut camp = vec![[0i64; 4]; ncamp_tot * ndays];
    for p in &pouts {
        for (a, b) in camp.iter_mut().zip(p.camp.iter()) {
            for k in 0..4 {
                a[k] += b[k];
            }
        }
    }
    let mut cc = output::CampCols {
        campaign_id: vec![],
        channel: vec![],
        day: vec![],
        sessions: vec![],
        purchasing: vec![],
        revenue: vec![],
        assisted: vec![],
    };
    for c in 0..ncamp_tot {
        for d in 0..ndays {
            let s = camp[c * ndays + d];
            if s[0] > 0 || s[3] > 0 {
                let (id, ch): (&[u8], &[u8]) =
                    if c == ctx.ncamp() { (b"organic", b"organic") } else { (&ctx.camp_ids[c], &ctx.channels[c]) };
                cc.campaign_id.push(ByteArray::from(id.to_vec()));
                cc.channel.push(ByteArray::from(ch.to_vec()));
                cc.day.push((day0 + d as i64) as i32);
                cc.sessions.push(s[0]);
                cc.purchasing.push(s[1]);
                cc.revenue.push(s[2]);
                cc.assisted.push(s[3]);
            }
        }
    }

    let rows_parts: Vec<Vec<URow>> = pouts.into_iter().map(|p| p.rows).collect();
    std::thread::scope(|s| {
        let cdp = &cd;
        let ccr = &cc;
        s.spawn(move || output::write_campaign_daily(&cdp.join("part-0.parquet"), ccr).unwrap());
        for (i, rows) in rows_parts.iter().enumerate() {
            let udp = &ud;
            let ctx = &ctx;
            s.spawn(move || {
                let cat_bytes: Vec<ByteArray> = ctx.cat_names.iter().map(|c| ByteArray::from(c.clone())).collect();
                let mut c = output::UserCols {
                    user_id: Vec::with_capacity(rows.len()),
                    day: Vec::with_capacity(rows.len()),
                    sessions: Vec::with_capacity(rows.len()),
                    events: Vec::with_capacity(rows.len()),
                    revenue: Vec::with_capacity(rows.len()),
                    top: Vec::new(),
                    top_def: Vec::with_capacity(rows.len()),
                };
                for r in rows {
                    c.user_id.push(ByteArray::from(key_bytes(r.pkey)));
                    c.day.push(r.day);
                    c.sessions.push(r.sessions);
                    c.events.push(r.events);
                    c.revenue.push(r.revenue);
                    if r.top == u16::MAX {
                        c.top_def.push(0);
                    } else {
                        c.top_def.push(1);
                        c.top.push(cat_bytes[r.top as usize].clone());
                    }
                }
                output::write_user_daily(&udp.join(format!("part-{}.parquet", i)), &c).unwrap();
            });
        }
    });
    log(t0, &format!("wrote {} user_daily rows, {} campaign_daily rows", nrows, cc.day.len()));
}

fn build_links(lists: &[(Vec<(u128, u128)>, Vec<u8>)], npairs: usize) -> KeyMap<u128> {
    let mut ids = KeyMap::<u32>::with_capacity(npairs + 16);
    let mut keys: Vec<u128> = Vec::with_capacity(npairs + 16);
    let mut parent: Vec<u32> = Vec::with_capacity(npairs + 16);
    let mut size: Vec<u32> = Vec::with_capacity(npairs + 16);
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let p = parent[x as usize];
            parent[x as usize] = parent[p as usize];
            x = parent[x as usize];
        }
        x
    }
    for (l, _) in lists {
        for &(a, b) in l {
            let mut idx = |k: u128| -> u32 {
                let (id, new) = ids.get_or_insert(k, keys.len() as u32);
                if new {
                    keys.push(k);
                    parent.push(id);
                    size.push(1);
                }
                id
            };
            let ia = idx(a);
            let ib = idx(b);
            let ra = find(&mut parent, ia);
            let rb = find(&mut parent, ib);
            if ra != rb {
                let (big, small) = if size[ra as usize] >= size[rb as usize] { (ra, rb) } else { (rb, ra) };
                parent[small as usize] = big;
                size[big as usize] += size[small as usize];
            }
        }
    }
    let n = keys.len();
    let mut minkey = vec![0u128; n];
    for i in 0..n {
        let r = find(&mut parent, i as u32) as usize;
        let k = keys[i];
        if minkey[r] == 0 || cmp_key(k, minkey[r]) == std::cmp::Ordering::Less {
            minkey[r] = k;
        }
    }
    let mut out = KeyMap::<u128>::with_capacity(n);
    for i in 0..n {
        let r = find(&mut parent, i as u32) as usize;
        out.get_or_insert(keys[i], minkey[r]);
    }
    out
}

#[inline(always)]
fn ev_bucket(h: u64) -> usize {
    (h >> (64 - EV_BITS)) as usize
}

#[inline(always)]
fn p_bucket(k: u128) -> usize {
    (hash128(k ^ 0x5bd1_e995_5bd1_e995) >> (64 - P_BITS)) as usize
}

const RING: usize = 16;

#[derive(Clone, Copy, Default)]
struct PendEv {
    ekey: u128,
    ukey: u128,
    pidk: u128,
    ts: i64,
    ingest: i64,
    hu: u64,
    hp: u64,
    price: u32,
    kind: u8,
}

fn parse_events(d: &[u8], s: usize, e: usize, ctx: &Ctx, out: &mut Parts<EvRec, 8>) {
    let end = d.as_ptr_range().end;
    let mut ring = [PendEv::default(); RING];
    let mut cnt = 0usize;
    let finish = |p: &PendEv, out: &mut Parts<EvRec, 8>| {
        let pkey = ctx.link.get_h(p.ukey, p.hu).unwrap_or(p.ukey);
        let cat = if p.kind == K_PURCHASE { ctx.products.get_h(p.pidk, p.hp).unwrap_or(ctx.unknown_cat) } else { 0 };
        out.push(
            ev_bucket(hash128(p.ekey)),
            EvRec { ekey: p.ekey, pkey, ts: p.ts, ingest: p.ingest, price: p.price, cat, kind: p.kind },
        );
    };
    for_rows::<7>(d, s, e, |f| {
        let [eid, uid, ts, ing, et, pid, price] = *f;
        if eid.is_empty() {
            return;
        }
        let ekey = key_fast(eid, end);
        let ingest = parse_ts(ing).unwrap_or(i64::MAX);
        'v: {
            if uid.is_empty() || pid.is_empty() {
                break 'v;
            }
            let t = match parse_ts(ts) {
                Some(t) if t >= ctx.ws && t < ctx.we => t,
                _ => break 'v,
            };
            let kind = match et {
                b"view" => K_VIEW,
                b"cart" => K_CART,
                b"purchase" => K_PURCHASE,
                _ => break 'v,
            };
            let mut pr = 0;
            let mut pidk = 0;
            let mut hp = 0;
            if kind == K_PURCHASE {
                pr = match parse_price(price) {
                    Some(p) => p,
                    None => break 'v,
                };
                pidk = key_fast(pid, end);
                hp = hash128(pidk);
                ctx.products.prefetch(hp);
            }
            if ingest.saturating_sub(t) > DAY_MS {
                break 'v;
            }
            let ukey = key_fast(uid, end);
            let hu = hash128(ukey);
            ctx.link.prefetch(hu);
            let slot = cnt % RING;
            if cnt >= RING {
                finish(&ring[slot], out);
            }
            ring[slot] = PendEv { ekey, ukey, pidk, ts: t, ingest, hu, hp, price: pr, kind };
            cnt += 1;
            return;
        }
        out.push(ev_bucket(hash128(ekey)), EvRec { ekey, ingest, ..Default::default() });
    });
    let start = cnt.saturating_sub(RING);
    for i in start..cnt {
        finish(&ring[i % RING], out);
    }
}

#[derive(Clone, Copy, Default)]
struct PendCl {
    ukey: u128,
    ckey: u128,
    ts: i64,
    hu: u64,
    camp: u32,
}

fn parse_clicks(d: &[u8], s: usize, e: usize, ctx: &Ctx, out: &mut Parts<ClRec, 4>) {
    let end = d.as_ptr_range().end;
    let lo = ctx.ws - WEEK_MS;
    let mut ring = [PendCl::default(); RING];
    let mut cnt = 0usize;
    let finish = |p: &PendCl, out: &mut Parts<ClRec, 4>| {
        let pkey = ctx.link.get_h(p.ukey, p.hu).unwrap_or(p.ukey);
        out.push(p_bucket(pkey), ClRec { pkey, ckey: p.ckey, ts: p.ts, camp: p.camp });
    };
    for_rows::<4>(d, s, e, |f| {
        let [cid, uid, camp, ts] = *f;
        if cid.is_empty() || uid.is_empty() {
            return;
        }
        let t = match parse_ts(ts) {
            Some(t) if t >= lo && t < ctx.we => t,
            _ => return,
        };
        let camp = match ctx.camps.get(key_fast(camp, end)) {
            Some(c) => c,
            None => return,
        };
        let ukey = key_fast(uid, end);
        let hu = hash128(ukey);
        ctx.link.prefetch(hu);
        let slot = cnt % RING;
        if cnt >= RING {
            finish(&ring[slot], out);
        }
        ring[slot] = PendCl { ukey, ckey: key_fast(cid, end), ts: t, hu, camp };
        cnt += 1;
    });
    let start = cnt.saturating_sub(RING);
    for i in start..cnt {
        finish(&ring[i % RING], out);
    }
}

fn dedup_bucket(parts: &[Vec<Vec<EvRec>>], b: usize, out: &mut Parts<EvOut, 8>, table: &mut Vec<u64>, losers: &mut Vec<u64>) {
    let n: usize = parts.iter().map(|p| p[b].len()).sum();
    if n == 0 {
        return;
    }
    let cap = (n * 2).next_power_of_two();
    let mask = cap - 1;
    table.clear();
    table.resize(cap, u64::MAX);
    losers.clear();
    losers.resize(n / 64 + 1, 0);
    // global position of a record = base[t] + i
    let mut base = Vec::with_capacity(parts.len());
    let mut acc = 0u32;
    for p in parts {
        base.push(acc);
        acc += p[b].len() as u32;
    }
    let get = |r: u32| -> &EvRec { &parts[(r >> 27) as usize][b][(r & ((1 << 27) - 1)) as usize] };
    let gpos = |r: u32| -> usize { base[(r >> 27) as usize] as usize + (r & ((1 << 27) - 1)) as usize };
    for (t, p) in parts.iter().enumerate() {
        for (i, rec) in p[b].iter().enumerate() {
            let r = ((t as u32) << 27) | i as u32;
            let h = hash128(rec.ekey);
            let fp = (h >> 20) as u32;
            let mut s = (h as usize) & mask;
            loop {
                let cur = table[s];
                if cur == u64::MAX {
                    table[s] = (r as u64) | ((fp as u64) << 32);
                    break;
                }
                if (cur >> 32) as u32 == fp {
                    let cr = cur as u32;
                    let o = get(cr);
                    if o.ekey == rec.ekey {
                        if rec.ingest < o.ingest {
                            table[s] = (r as u64) | ((fp as u64) << 32);
                            let g = gpos(cr);
                            losers[g / 64] |= 1 << (g % 64);
                        } else {
                            let g = base[t] as usize + i;
                            losers[g / 64] |= 1 << (g % 64);
                        }
                        break;
                    }
                }
                s = (s + 1) & mask;
            }
        }
    }
    for (t, p) in parts.iter().enumerate() {
        let bt = base[t] as usize;
        for (i, rec) in p[b].iter().enumerate() {
            let g = bt + i;
            if rec.kind != K_INVALID && (losers[g / 64] >> (g % 64)) & 1 == 0 {
                out.push(p_bucket(rec.pkey), EvOut { pkey: rec.pkey, ts: rec.ts, price: rec.price, cat: rec.cat, kind: rec.kind });
            }
        }
    }
}

struct Scratch {
    starts: Vec<i64>,
    days: Vec<u32>,
    runs: Vec<(u32, u32, u32)>, // (day, first session, end session)
    ct: Vec<(u32, i64)>,
    catsum: Vec<i64>,
    touched: Vec<u16>,
}

impl Scratch {
    fn new(ncat: usize) -> Self {
        Scratch { starts: vec![], days: vec![], runs: vec![], ct: vec![], catsum: vec![0; ncat], touched: vec![] }
    }
}

fn process_bucket(evs: &mut [EvOut], cls: &mut [ClRec], ctx: &Ctx, sc: &mut Scratch, rows: &mut Vec<URow>, camp: &mut [[i64; 4]]) {
    evs.sort_unstable_by(|a, b| a.pkey.cmp(&b.pkey).then(a.ts.cmp(&b.ts)));
    cls.sort_unstable_by(|a, b| a.pkey.cmp(&b.pkey).then(a.ts.cmp(&b.ts)).then_with(|| cmp_key(a.ckey, b.ckey)));
    let mut ci = 0;
    let mut i = 0;
    while i < evs.len() {
        let p = evs[i].pkey;
        let mut j = i + 1;
        while j < evs.len() && evs[j].pkey == p {
            j += 1;
        }
        while ci < cls.len() && cls[ci].pkey < p {
            ci += 1;
        }
        let mut cj = ci;
        while cj < cls.len() && cls[cj].pkey == p {
            cj += 1;
        }
        process_person(p, &evs[i..j], &cls[ci..cj], ctx, sc, rows, camp);
        i = j;
        ci = cj;
    }
}

#[inline]
fn credit(cls: &[ClRec], start: i64, organic: usize) -> usize {
    let idx = cls.partition_point(|c| c.ts <= start);
    if idx == 0 {
        return organic;
    }
    let t = cls[idx - 1].ts;
    if start - t > WEEK_MS {
        return organic;
    }
    let first = cls[..idx].partition_point(|c| c.ts < t);
    cls[first].camp as usize
}

fn process_person(p: u128, evs: &[EvOut], cls: &[ClRec], ctx: &Ctx, sc: &mut Scratch, rows: &mut Vec<URow>, camp: &mut [[i64; 4]]) {
    let ndays = ctx.ndays;
    let organic = ctx.ncamp();
    sc.starts.clear();
    sc.days.clear();
    let n = evs.len();
    let mut k = 0;
    let mut cur_day = u32::MAX;
    let (mut d_sess, mut d_ev, mut d_rev) = (0i64, 0i64, 0i64);

    macro_rules! flush {
        () => {
            if cur_day != u32::MAX {
                let mut top = u16::MAX;
                let mut best = 0i64;
                for &c in &sc.touched {
                    let s = sc.catsum[c as usize];
                    if s > best || (s == best && c < top) {
                        best = s;
                        top = c;
                    }
                    sc.catsum[c as usize] = 0;
                }
                sc.touched.clear();
                rows.push(URow { pkey: p, day: (ctx.day0 + cur_day as i64) as i32, sessions: d_sess, events: d_ev, revenue: d_rev, top });
            }
        };
    }

    while k < n {
        let start = evs[k].ts;
        let mut last = start;
        let mut cnt = 0i64;
        let mut rev = 0i64;
        let mut purch = false;
        let day = (start.div_euclid(DAY_MS) - ctx.day0) as u32;
        if day != cur_day {
            flush!();
            cur_day = day;
            d_sess = 0;
            d_ev = 0;
            d_rev = 0;
        }
        while k < n && evs[k].ts - last <= GAP_MS {
            let e = &evs[k];
            last = e.ts;
            cnt += 1;
            if e.kind == K_PURCHASE {
                rev += e.price as i64;
                purch = true;
                let c = e.cat as usize;
                if sc.catsum[c] == 0 {
                    sc.touched.push(e.cat);
                }
                sc.catsum[c] += e.price as i64;
            }
            k += 1;
        }
        d_sess += 1;
        d_ev += cnt;
        d_rev += rev;
        let c = credit(cls, start, organic);
        let slot = &mut camp[c * ndays + day as usize];
        slot[0] += 1;
        slot[1] += purch as i64;
        slot[2] += rev;
        sc.starts.push(start);
        sc.days.push(day);
    }
    flush!();

    if cls.is_empty() {
        return;
    }
    // ---- assists ----
    sc.runs.clear();
    for (i, &d) in sc.days.iter().enumerate() {
        match sc.runs.last_mut() {
            Some(r) if r.0 == d => r.2 = i as u32 + 1,
            _ => sc.runs.push((d, i as u32, i as u32 + 1)),
        }
    }
    sc.ct.clear();
    sc.ct.extend(cls.iter().map(|c| (c.camp, c.ts)));
    sc.ct.sort_unstable();
    let starts = &sc.starts;
    let runs = &sc.runs;
    let mut add_range = |cp: u32, a: i64, b: i64| {
        let mut i = starts.partition_point(|&s| s < a);
        let j = starts.partition_point(|&s| s <= b);
        if i >= j {
            return;
        }
        let mut ri = runs.partition_point(|r| r.2 as usize <= i);
        while i < j {
            let r = runs[ri];
            let e = (r.2 as usize).min(j);
            camp[cp as usize * ndays + r.0 as usize][3] += (e - i) as i64;
            i = e;
            ri += 1;
        }
    };
    let ct = &sc.ct;
    let mut g = 0;
    while g < ct.len() {
        let cp = ct[g].0;
        let mut a = ct[g].1;
        let mut b = a + WEEK_MS;
        let mut h = g + 1;
        while h < ct.len() && ct[h].0 == cp {
            let t = ct[h].1;
            if t <= b {
                b = b.max(t + WEEK_MS);
            } else {
                add_range(cp, a, b);
                a = t;
                b = t + WEEK_MS;
            }
            h += 1;
        }
        add_range(cp, a, b);
        g = h;
    }
}
