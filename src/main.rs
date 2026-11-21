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
mod part;
mod pq;
mod util;

#[global_allocator]
static GLOBAL: alloc::BigAlloc = alloc::BigAlloc;

use part::{BlockVec, Parts};
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering as AO};
use std::time::Instant;
use util::*;

// bucket counts (log2), chosen from the input size so buckets stay cache-sized
static EV_BITS: AtomicU32 = AtomicU32::new(9); // dedup buckets
static P_BITS: AtomicU32 = AtomicU32::new(10); // person buckets
const CHUNK: usize = 4 << 20;
const PAD: usize = 256;
const PIECE: usize = 256 << 10;

const K_INVALID: u8 = 0;
const K_VIEW: u8 = 1;
const K_CART: u8 = 2;
const K_PURCHASE: u8 = 3;

/// Dedup key stream: event_id and ingest_ts (24 bytes). The payload stream
/// (EvOut) is pushed in lockstep, so index i in both refers to one row.
#[derive(Clone, Copy, Default)]
struct DKey {
    k0: u64,
    k1: u64,
    ingest: i64,
    /// where this row's payload went: (parse worker, person bucket, index), or NOLOC
    loc: u64,
}

const NOLOC: u64 = u64::MAX;

#[inline(always)]
fn mkloc(t: usize, pb: usize, i: usize) -> u64 {
    ((t as u64) << 59) | ((pb as u64) << 40) | i as u64
}

impl DKey {
    #[inline(always)]
    fn ekey(&self) -> u128 {
        ((self.k1 as u128) << 64) | self.k0 as u128
    }
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
    link: LinkMap,
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

/// (wall, user, sys) at each log point, for the probe summary
static MARKS: Mutex<Vec<(f64, f64, f64)>> = Mutex::new(Vec::new());

fn log(t0: Instant, msg: &str) {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let tv = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    MARKS.lock().unwrap().push((t0.elapsed().as_secs_f64(), tv(ru.ru_utime), tv(ru.ru_stime)));
    eprintln!(
        "[{:8.3}s] {}  (user {:.2}s sys {:.2}s minflt {})",
        t0.elapsed().as_secs_f64(),
        msg,
        tv(ru.ru_utime),
        tv(ru.ru_stime),
        ru.ru_minflt
    );
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
    maps: Vec<Mm>,
}

/// Read-only file mapping (used instead of pread when MMAPIO is set).
struct Mm {
    p: usize,
    n: usize,
}

impl Mm {
    fn new(f: &File, n: usize) -> Mm {
        use std::os::unix::io::AsRawFd;
        let p = unsafe { libc::mmap(std::ptr::null_mut(), n, libc::PROT_READ, libc::MAP_PRIVATE, f.as_raw_fd(), 0) };
        assert!(p != libc::MAP_FAILED, "mmap input failed");
        Mm { p: p as usize, n }
    }
    fn bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.p as *const u8, self.n) }
    }
}

/// input via mmap instead of pread (scorer: mapping cost is negligible)
static MMAPIO: std::sync::LazyLock<bool> = std::sync::LazyLock::new(|| std::env::var_os("MMAPIO").is_some());

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
        let maps = if *MMAPIO { files.iter().map(|(f, n)| Mm::new(f, *n)).collect() } else { Vec::new() };
        Input { files, maps }
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
        let mut extra = 4 << 10;
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
    if let Some(dir) = std::env::var_os("PROBE_MICRO") {
        probe_micro(Path::new(&dir));
        return;
    }
    if std::env::var_os("NOHUGE").is_some() {
        alloc::HUGE.store(false, AO::Relaxed);
    }
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
        || LinkParse { pairs: Vec::new(), keys: (0..1usize << L_BITS).map(|_| Vec::new()).collect(), buf: Vec::new() },
        |lp, i| {
            let (fi, s, e) = link_chunks[i];
            let (s, e) = links_in.read(fi, s, e, &mut lp.buf);
            let d = &lp.buf[..];
            let end = d.as_ptr_range().end;
            let (pairs, keys) = (&mut lp.pairs, &mut lp.keys);
            for_rows::<2>(d, s, e, |f| {
                if !f[0].is_empty() && !f[1].is_empty() {
                    let a = key_fast(f[0], end);
                    let b = key_fast(f[1], end);
                    pairs.push((a, b));
                    let ha = hash128(a);
                    let hb = hash128(b);
                    keys[(ha >> (64 - L_BITS)) as usize].push(a);
                    keys[(hb >> (64 - L_BITS)) as usize].push(b);
                }
            });
        },
    );
    let npairs: usize = pair_lists.iter().map(|v| v.pairs.len()).sum();
    let link = build_links(nthreads, &pair_lists);
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
    // ~20M events (1.7 GB) -> 512 / 1024 buckets; ~60M -> 1024 / 2048
    if ev_bytes > (3usize << 30) {
        EV_BITS.store(10, AO::Relaxed);
        P_BITS.store(11, AO::Relaxed);
    }
    let nev_b = 1usize << EV_BITS.load(AO::Relaxed);
    let np_b = 1usize << P_BITS.load(AO::Relaxed);
    let ntask = ev_chunks.len() + cl_chunks.len();

    struct ParseOut {
        id: usize,
        evk: Parts<DKey, 8>,
        evp: Parts<EvOut, 8>,
        cl: Parts<ClRec, 4>,
        buf: Vec<u8>,
    }
    let next_id = AtomicUsize::new(0);
    let parsed = par_for(
        nthreads,
        ntask,
        || ParseOut {
            id: next_id.fetch_add(1, AO::Relaxed),
            evk: Parts::new(nev_b),
            evp: Parts::new(np_b),
            cl: Parts::new(np_b),
            buf: Vec::new(),
        },
        |po, i| {
            // read in cache-sized pieces so the buffer stays in L2
            if i < ev_chunks.len() {
                let (fi, s0, e0) = ev_chunks[i];
                if *MMAPIO {
                    parse_events(ev_in.maps[fi].bytes(), s0, e0, &ctx, po.id, &mut po.evk, &mut po.evp);
                    return;
                }
                let mut p = s0;
                while p < e0 {
                    let q = (p + PIECE).min(e0);
                    let (s, e) = ev_in.read(fi, p, q, &mut po.buf);
                    parse_events(&po.buf, s, e, &ctx, po.id, &mut po.evk, &mut po.evp);
                    p = q;
                }
            } else {
                let (fi, s0, e0) = cl_chunks[i - ev_chunks.len()];
                if *MMAPIO {
                    parse_clicks(cl_in.maps[fi].bytes(), s0, e0, &ctx, &mut po.cl);
                    return;
                }
                let mut p = s0;
                while p < e0 {
                    let q = (p + PIECE).min(e0);
                    let (s, e) = cl_in.read(fi, p, q, &mut po.buf);
                    parse_clicks(&po.buf, s, e, &ctx, &mut po.cl);
                    p = q;
                }
            }
        },
    );
    // dedup keys go bucket-major (each dedup task takes its bucket and hands
    // the blocks back); payloads stay per worker until dedup has marked losers
    let ev_b: Vec<Mutex<Vec<BlockVec<DKey>>>> = (0..nev_b).map(|_| Mutex::new(Vec::new())).collect();
    let cl_b: Vec<Mutex<Vec<BlockVec<ClRec>>>> = (0..np_b).map(|_| Mutex::new(Vec::new())).collect();
    let mut parsed = parsed;
    parsed.sort_by_key(|p| p.id);
    let mut pay: Vec<Vec<BlockVec<EvOut>>> = Vec::with_capacity(parsed.len());
    let (mut nrec, mut ncl) = (0usize, 0usize);
    for p in parsed {
        for (b, v) in p.evk.finish().into_iter().enumerate() {
            nrec += v.len();
            ev_b[b].lock().unwrap().push(v);
        }
        pay.push(p.evp.finish());
        for (b, v) in p.cl.finish().into_iter().enumerate() {
            ncl += v.len();
            cl_b[b].lock().unwrap().push(v);
        }
    }
    log(t0, &format!("parsed: {} event rows, {} valid clicks ({} + {} bytes)", nrec, ncl, ev_bytes, cl_bytes));

    // ---- dedup: mark every copy but the earliest as dead, in place ----
    let killed: usize = par_for(
        nthreads,
        nev_b,
        || (0usize, Vec::<u64>::new(), Vec::<u64>::new()),
        |(k, table, losers), b| {
            let ks = std::mem::take(&mut *ev_b[b].lock().unwrap());
            *k += dedup_bucket(&ks, &pay, table, losers);
            ks.into_iter().for_each(BlockVec::release);
        },
    )
    .into_iter()
    .map(|x| x.0)
    .sum();
    let pe_b: Vec<Mutex<Vec<BlockVec<EvOut>>>> = (0..np_b).map(|_| Mutex::new(Vec::new())).collect();
    let mut nsurv = 0usize;
    for w in pay {
        for (b, v) in w.into_iter().enumerate() {
            nsurv += v.len();
            pe_b[b].lock().unwrap().push(v);
        }
    }
    log(t0, &format!("dedup: {} valid rows, {} later copies dropped", nsurv, killed));

    // ---- per person ----
    struct POut {
        rows: Vec<URow>,
        camp: Vec<[i64; 4]>,
        sc: Scratch,
    }
    let ncamp_tot = ctx.ncamp() + 1;
    let pouts = par_for(
        nthreads,
        np_b,
        || POut {
            rows: Vec::new(),
            camp: vec![[0i64; 4]; ncamp_tot * ndays],
            sc: Scratch::new(ctx.cat_names.len()),
        },
        |po, b| {
            let ev = std::mem::take(&mut *pe_b[b].lock().unwrap());
            let cl = std::mem::take(&mut *cl_b[b].lock().unwrap());
            {
                let evs: Vec<&[EvOut]> = ev.iter().flat_map(|v| v.slices()).collect();
                let cls: Vec<&[ClRec]> = cl.iter().flat_map(|v| v.slices()).collect();
                process_bucket(&evs, &cls, &ctx, &mut po.sc, &mut po.rows, &mut po.camp);
            }
            ev.into_iter().for_each(BlockVec::release);
            cl.into_iter().for_each(BlockVec::release);
        },
    );
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
    let mut cc: Vec<pq::Column> = vec![
        pq::Column::new("campaign_id", pq::Kind::Str, 0),
        pq::Column::new("channel", pq::Kind::Str, 0),
        pq::Column::new("day", pq::Kind::Date, 0),
        pq::Column::new("sessions", pq::Kind::I64, 0),
        pq::Column::new("purchasing_sessions", pq::Kind::I64, 0),
        pq::Column::new("revenue_cents", pq::Kind::I64, 0),
        pq::Column::new("assisted_sessions", pq::Kind::I64, 0),
    ];
    let mut ncrows = 0usize;
    for c in 0..ncamp_tot {
        for d in 0..ndays {
            let s = camp[c * ndays + d];
            if s[0] > 0 || s[3] > 0 {
                let (id, ch): (&[u8], &[u8]) =
                    if c == ctx.ncamp() { (b"organic", b"organic") } else { (&ctx.camp_ids[c], &ctx.channels[c]) };
                cc[0].push_str(id);
                cc[1].push_str(ch);
                cc[2].push_i32((day0 + d as i64) as i32);
                cc[3].push_i64(s[0]);
                cc[4].push_i64(s[1]);
                cc[5].push_i64(s[2]);
                cc[6].push_i64(s[3]);
                ncrows += 1;
            }
        }
    }

    let rows_parts: Vec<Vec<URow>> = pouts.into_iter().map(|p| p.rows).collect();
    std::thread::scope(|s| {
        let cdp = &cd;
        let ccr = &cc;
        s.spawn(move || pq::write(&cdp.join("part-0.parquet"), ncrows, ccr).expect("write campaign_daily"));
        for (i, rows) in rows_parts.iter().enumerate() {
            let udp = &ud;
            let ctx = &ctx;
            s.spawn(move || {
                let n = rows.len();
                let mut c = vec![
                    pq::Column::new("user_id", pq::Kind::Str, n),
                    pq::Column::new("day", pq::Kind::Date, n),
                    pq::Column::new("sessions", pq::Kind::I64, n),
                    pq::Column::new("events", pq::Kind::I64, n),
                    pq::Column::new("revenue_cents", pq::Kind::I64, n),
                    pq::Column::new("top_category", pq::Kind::OptStr, n),
                ];
                for r in rows {
                    if is_long(r.pkey) {
                        c[0].push_str(&key_bytes(r.pkey));
                    } else {
                        let a = r.pkey.to_be_bytes();
                        c[0].push_str(&a[..a[15] as usize]);
                    }
                    c[1].push_i32(r.day);
                    c[2].push_i64(r.sessions);
                    c[3].push_i64(r.events);
                    c[4].push_i64(r.revenue);
                    c[5].push_opt_str(if r.top == u16::MAX { None } else { Some(&ctx.cat_names[r.top as usize]) });
                }
                pq::write(&udp.join(format!("part-{}.parquet", i)), n, &c).expect("write user_daily");
            });
        }
    });
    log(t0, &format!("wrote {} user_daily rows, {} campaign_daily rows", nrows, ncrows));
    probe_hooks(nthreads, &cd);
}

const L_BITS: u32 = 6;

/// ID -> person name, sharded by the top hash bits so it can be built in parallel.
struct LinkMap {
    shards: Vec<KeyMap<u128>>,
    len: usize,
}

impl LinkMap {
    #[inline(always)]
    fn shard(&self, h: u64) -> &KeyMap<u128> {
        unsafe { self.shards.get_unchecked((h >> (64 - L_BITS)) as usize) }
    }
    #[inline(always)]
    fn prefetch(&self, h: u64) {
        self.shard(h).prefetch(h)
    }
    #[inline(always)]
    fn get_h(&self, k: u128, h: u64) -> Option<u128> {
        self.shard(h).get_h(k, h)
    }
    fn len(&self) -> usize {
        self.len
    }
}

/// Collects results of f(task) in task order.
fn par_map<R: Send, F: Fn(usize) -> R + Sync>(nthreads: usize, ntasks: usize, f: F) -> Vec<R> {
    let mut v: Vec<(usize, R)> = par_for(nthreads, ntasks, Vec::new, |acc: &mut Vec<(usize, R)>, i| acc.push((i, f(i))))
        .into_iter()
        .flatten()
        .collect();
    v.sort_unstable_by_key(|x| x.0);
    v.into_iter().map(|x| x.1).collect()
}

fn uf_find(p: &[AtomicU32], mut x: u32) -> u32 {
    loop {
        let px = p[x as usize].load(AO::Acquire);
        if px == x {
            return x;
        }
        let ppx = p[px as usize].load(AO::Acquire);
        if ppx != px {
            let _ = p[x as usize].compare_exchange(px, ppx, AO::AcqRel, AO::Relaxed);
        }
        x = ppx;
    }
}

fn uf_union(p: &[AtomicU32], a: u32, b: u32) {
    let (mut a, mut b) = (a, b);
    loop {
        a = uf_find(p, a);
        b = uf_find(p, b);
        if a == b {
            return;
        }
        let (hi, lo) = if a > b { (a, b) } else { (b, a) };
        if p[hi as usize].compare_exchange(hi, lo, AO::AcqRel, AO::Relaxed).is_ok() {
            return;
        }
    }
}

/// lists: per worker, (pairs, keys per shard)
/// Diagnostics only (used by probe.sh, never by a real run):
/// PROBE_OUT=file appends a one-line phase summary of this run to file;
/// PROBE_NOTES=file adds its lines as extra campaign_daily rows.
fn probe_hooks(nthreads: usize, cd: &Path) {
    if let Some(path) = std::env::var_os("PROBE_OUT") {
        let m = MARKS.lock().unwrap();
        let tag = std::env::var("PROBE_TAG").unwrap_or_default();
        let mut line = format!("T{}{}", nthreads, tag);
        let names = ["", "L", "P", "D", "S", "W"];
        let f2 = |x: f64| format!("{:.2}", x).trim_start_matches('0').to_string();
        for i in 1..m.len().min(names.len()) {
            let (w, u, s) = (m[i].0 - m[i - 1].0, m[i].1 - m[i - 1].1, m[i].2 - m[i - 1].2);
            if names[i] == "P" || names[i] == "S" {
                line += &format!(" {}{}/{}/{}", names[i], f2(w), f2(u), f2(s));
            } else {
                line += &format!(" {}{}", names[i], f2(w));
            }
        }
        if let Some(l) = m.last() {
            line += &format!(" ={} u{:.1} s{:.1}", f2(l.0), l.1, l.2);
        }
        use std::io::Write;
        if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(path) {
            let _ = writeln!(f, "{}", line);
        }
    }
    if let Some(path) = std::env::var_os("PROBE_NOTES") {
        let text = std::fs::read_to_string(path).unwrap_or_default();
        let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
        let mut c = vec![
            pq::Column::new("campaign_id", pq::Kind::Str, 0),
            pq::Column::new("channel", pq::Kind::Str, 0),
            pq::Column::new("day", pq::Kind::Date, 0),
            pq::Column::new("sessions", pq::Kind::I64, 0),
            pq::Column::new("purchasing_sessions", pq::Kind::I64, 0),
            pq::Column::new("revenue_cents", pq::Kind::I64, 0),
            pq::Column::new("assisted_sessions", pq::Kind::I64, 0),
        ];
        for (i, l) in lines.iter().enumerate() {
            c[0].push_str(format!("{} {}", i, l).as_bytes());
            c[1].push_str(b"probe");
            c[2].push_i32(0);
            for k in 3..7 {
                c[k].push_i64(0);
            }
        }
        let _ = pq::write(&cd.join("zz-probe.parquet"), lines.len(), &c);
    }
}

/// Diagnostics only: micro-benchmarks of the machine primitives the pipeline
/// leans on, appended as one line to PROBE_OUT.
fn probe_micro(dir: &Path) {
    let ms = |t: Instant| t.elapsed().as_secs_f64() * 1000.0;
    let mut out = String::from("M");
    let n = 512usize << 20;
    let map = |huge: bool| unsafe {
        let p = libc::mmap(std::ptr::null_mut(), n, libc::PROT_READ | libc::PROT_WRITE, libc::MAP_PRIVATE | libc::MAP_ANONYMOUS, -1, 0);
        if huge {
            libc::madvise(p, n, libc::MADV_HUGEPAGE);
        }
        p as *mut u8
    };
    let touch = |p: *mut u8| {
        let mut i = 0;
        while i < n {
            unsafe { p.add(i).write_volatile(1) };
            i += 4096;
        }
    };
    // fresh anonymous memory: 4K pages, then huge pages, then rewriting faulted memory
    let p = map(false);
    let t = Instant::now();
    touch(p);
    out += &format!(" f4k={:.0}", ms(t));
    let q = map(true);
    let t = Instant::now();
    touch(q);
    out += &format!(" fthp={:.0}", ms(t));
    let t = Instant::now();
    unsafe { std::ptr::write_bytes(q, 7, n) };
    out += &format!(" mset={:.0}", ms(t));
    // fresh memory touched by 8 threads at once (fault scalability)
    let r = map(true) as usize;
    let t = Instant::now();
    std::thread::scope(|sc| {
        for k in 0..8 {
            sc.spawn(move || {
                let mut i = k * (n / 8);
                while i < (k + 1) * (n / 8) {
                    unsafe { (r as *mut u8).add(i).write_volatile(1) };
                    i += 4096;
                }
            });
        }
    });
    out += &format!(" fthp8={:.0}", ms(t));
    unsafe {
        libc::munmap(p as *mut libc::c_void, n);
        libc::munmap(q as *mut libc::c_void, n);
        libc::munmap(r as *mut libc::c_void, n);
    }
    // reading the events files: pread 1 thread, pread 8 threads, mmap+touch
    let ev = Input::open(&dir.join("events"));
    let chunks = ev.chunks();
    let mut buf = vec![0u8; PIECE];
    let t = Instant::now();
    let mut sum = 0usize;
    for &(fi, s, e) in &chunks {
        let mut p = s;
        while p < e {
            let k = (e - p).min(PIECE);
            ev.files[fi].0.read_exact_at(&mut buf[..k], p as u64).unwrap();
            sum += buf[k - 1] as usize;
            p += k;
        }
    }
    out += &format!(" pr1={:.0}", ms(t));
    let t = Instant::now();
    par_for(8, chunks.len(), || vec![0u8; PIECE], |b, i| {
        let (fi, s, e) = chunks[i];
        let mut p = s;
        while p < e {
            let k = (e - p).min(PIECE);
            ev.files[fi].0.read_exact_at(&mut b[..k], p as u64).unwrap();
            p += k;
        }
    });
    out += &format!(" pr8={:.0}", ms(t));
    let t = Instant::now();
    for (f, len) in &ev.files {
        use std::os::unix::io::AsRawFd;
        unsafe {
            let m = libc::mmap(std::ptr::null_mut(), *len, libc::PROT_READ, libc::MAP_PRIVATE, f.as_raw_fd(), 0) as *const u8;
            let mut i = 0;
            while i < *len {
                sum += *m.add(i) as usize;
                i += 4096;
            }
            libc::munmap(m as *mut libc::c_void, *len);
        }
    }
    out += &format!(" mm1={:.0}", ms(t));
    // thread spawn/join and raw syscall cost
    let t = Instant::now();
    for _ in 0..50 {
        std::thread::scope(|sc| {
            for _ in 0..8 {
                sc.spawn(|| ());
            }
        });
    }
    out += &format!(" sp50={:.0}", ms(t));
    let t = Instant::now();
    for _ in 0..20000 {
        unsafe { libc::getppid() };
    }
    out += &format!(" sc={:.0}ns", t.elapsed().as_nanos() as f64 / 20000.0);
    out += &format!(" gb={:.2}", ev.bytes() as f64 / 1e9);
    let _ = sum;
    if let Some(path) = std::env::var_os("PROBE_OUT") {
        use std::io::Write;
        if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(path) {
            let _ = writeln!(f, "{}", out);
        }
    }
    eprintln!("{}", out);
}

fn build_links(nthreads: usize, lists: &[LinkParse]) -> LinkMap {
    let ns = 1usize << L_BITS;
    // L1: dense ids per shard
    let shard_maps: Vec<(KeyMap<u128>, Vec<u128>)> = par_map(nthreads, ns, |s| {
        let n: usize = lists.iter().map(|l| l.keys[s].len()).sum();
        let mut m = KeyMap::<u128>::with_capacity(n + 16);
        let mut keys = Vec::with_capacity(n / 2 + 16);
        for l in lists {
            for &k in &l.keys[s] {
                let (_, new) = m.get_or_insert_h(k, hash128(k), keys.len() as u128);
                if new {
                    keys.push(k);
                }
            }
        }
        (m, keys)
    });
    let mut off = Vec::with_capacity(ns + 1);
    let mut acc = 0usize;
    for (_, k) in &shard_maps {
        off.push(acc);
        acc += k.len();
    }
    let n = acc;
    let mut keys: Vec<u128> = Vec::with_capacity(n);
    for (_, k) in &shard_maps {
        keys.extend_from_slice(k);
    }
    let gid = |k: u128| -> u32 {
        let h = hash128(k);
        let s = (h >> (64 - L_BITS)) as usize;
        (off[s] + shard_maps[s].0.get_h(k, h).unwrap() as usize) as u32
    };
    // L2: concurrent union-find
    let parent: Vec<AtomicU32> = (0..n as u32).map(AtomicU32::new).collect();
    par_for(nthreads, lists.len(), || (), |_, i| {
        for &(a, b) in &lists[i].pairs {
            uf_union(&parent, gid(a), gid(b));
        }
    });
    // L3: smallest key per component
    let minidx: Vec<AtomicU32> = (0..n).map(|_| AtomicU32::new(u32::MAX)).collect();
    let step = 1 << 16;
    par_for(nthreads, n.div_ceil(step), || (), |_, t| {
        for i in t * step..((t + 1) * step).min(n) {
            let r = uf_find(&parent, i as u32) as usize;
            let m = &minidx[r];
            let mut cur = m.load(AO::Acquire);
            loop {
                if cur != u32::MAX && cmp_key(keys[cur as usize], keys[i]) != std::cmp::Ordering::Greater {
                    break;
                }
                match m.compare_exchange(cur, i as u32, AO::AcqRel, AO::Acquire) {
                    Ok(_) => break,
                    Err(c) => cur = c,
                }
            }
        }
    });
    // L4: values become the person name
    let mut maps: Vec<KeyMap<u128>> = shard_maps.into_iter().map(|x| x.0).collect();
    let per = ns.div_ceil(nthreads);
    std::thread::scope(|sc| {
        for (ci, chunk) in maps.chunks_mut(per).enumerate() {
            let (parent, minidx, keys, off) = (&parent, &minidx, &keys, &off);
            sc.spawn(move || {
                for (j, m) in chunk.iter_mut().enumerate() {
                    let base = off[ci * per + j];
                    for slot in m.slots_mut() {
                        if slot.0 != 0 {
                            let g = base + slot.1 as usize;
                            let r = uf_find(parent, g as u32) as usize;
                            slot.1 = keys[minidx[r].load(AO::Relaxed) as usize];
                        }
                    }
                }
            });
        }
    });
    LinkMap { shards: maps, len: n }
}

struct LinkParse {
    pairs: Vec<(u128, u128)>,
    keys: Vec<Vec<u128>>,
    buf: Vec<u8>,
}

#[inline(always)]
fn ev_bucket(h: u64) -> usize {
    (h >> (64 - EV_BITS.load(AO::Relaxed))) as usize
}

/// same hash as the link map, so an unlinked user's hash is reused as-is
#[inline(always)]
fn p_hash(k: u128) -> u64 {
    hash128(k)
}


const RING: usize = 16;

#[derive(Clone, Copy, Default)]
struct PendEv {
    ekey: u128,
    ukey: u128,
    ts: i64,
    ingest: i64,
    hu: u64,
    price: u32,
    b: u16,
    cat: u16,
    kind: u8,
}

fn parse_events(d: &[u8], s: usize, e: usize, ctx: &Ctx, wid: usize, outk: &mut Parts<DKey, 8>, outp: &mut Parts<EvOut, 8>) {
    let end = d.as_ptr_range().end;
    let mut ring = [PendEv::default(); RING];
    let mut cnt = 0usize;
    let finish = |p: &PendEv, outk: &mut Parts<DKey, 8>, outp: &mut Parts<EvOut, 8>| {
        let (pkey, ph) = match ctx.link.get_h(p.ukey, p.hu) {
            Some(n) => (n, p_hash(n)),
            None => (p.ukey, p.hu),
        };
        let pb = (ph >> (64 - P_BITS.load(AO::Relaxed))) as usize;
        let i = outp.push_idx(pb, EvOut { pkey, ts: p.ts, price: p.price, cat: p.cat, kind: p.kind });
        let loc = mkloc(wid, pb, i);
        outk.push(p.b as usize, DKey { k0: p.ekey as u64, k1: (p.ekey >> 64) as u64, ingest: p.ingest, loc });
    };
    for_rows::<7>(d, s, e, |f| {
        let [eid, uid, ts, ing, et, pid, price] = *f;
        if eid.is_empty() {
            return;
        }
        let ekey = key_fast(eid, end);
        let ingest = parse_ts_fast(ing, end).unwrap_or(i64::MAX);
        'v: {
            if uid.is_empty() || pid.is_empty() {
                break 'v;
            }
            let t = match parse_ts_fast(ts, end) {
                Some(t) if t >= ctx.ws && t < ctx.we => t,
                _ => break 'v,
            };
            let kind = match et.len() {
                4 => match u32::from_le_bytes(et.try_into().unwrap()) {
                    0x7765_6976 => K_VIEW, // "view"
                    0x7472_6163 => K_CART, // "cart"
                    _ => break 'v,
                },
                8 if u64::from_le_bytes(et.try_into().unwrap()) == u64::from_le_bytes(*b"purchase") => K_PURCHASE,
                _ => break 'v,
            };
            let mut pr = 0;
            let mut cat = 0;
            if kind == K_PURCHASE {
                pr = match parse_price(price) {
                    Some(p) => p,
                    None => break 'v,
                };
                cat = ctx.products.get(key_fast(pid, end)).unwrap_or(ctx.unknown_cat);
            }
            if ingest.saturating_sub(t) > DAY_MS {
                break 'v;
            }
            let ukey = key_fast(uid, end);
            let hu = hash128(ukey);
            ctx.link.prefetch(hu);
            let slot = cnt % RING;
            if cnt >= RING {
                finish(&ring[slot], outk, outp);
            }
            let b = ev_bucket(hash128(ekey)) as u16;
            ring[slot] = PendEv { ekey, ukey, ts: t, ingest, hu, price: pr, b, cat, kind };
            cnt += 1;
            return;
        }
        let b = ev_bucket(hash128(ekey));
        outk.push(b, DKey { k0: ekey as u64, k1: (ekey >> 64) as u64, ingest, loc: NOLOC });
    });
    let start = cnt.saturating_sub(RING);
    for i in start..cnt {
        finish(&ring[i % RING], outk, outp);
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
        let (pkey, ph) = match ctx.link.get_h(p.ukey, p.hu) {
            Some(n) => (n, p_hash(n)),
            None => (p.ukey, p.hu),
        };
        out.push((ph >> (64 - P_BITS.load(AO::Relaxed))) as usize, ClRec { pkey, ckey: p.ckey, ts: p.ts, camp: p.camp });
    };
    for_rows::<4>(d, s, e, |f| {
        let [cid, uid, camp, ts] = *f;
        if cid.is_empty() || uid.is_empty() {
            return;
        }
        let t = match parse_ts_fast(ts, end) {
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

/// Finds, per event_id, the copy with the smallest ingest_ts and marks the
/// payloads of all other copies dead. Returns how many payloads it killed.
fn dedup_bucket(kparts: &[BlockVec<DKey>], pay: &[Vec<BlockVec<EvOut>>], table: &mut Vec<u64>, losers: &mut Vec<u64>) -> usize {
    let n: usize = kparts.iter().map(|p| p.len()).sum();
    if n == 0 {
        return 0;
    }
    let cap = (n * 2).next_power_of_two();
    let mask = cap - 1;
    table.clear();
    table.resize(cap, u64::MAX);
    losers.clear();
    losers.resize(n / 64 + 1, 0);
    // global position of a record = base[t] + i; table holds (fingerprint << 32 | position)
    let mut base = Vec::with_capacity(kparts.len() + 1);
    let mut acc = 0usize;
    for p in kparts {
        base.push(acc);
        acc += p.len();
    }
    base.push(acc);
    let get = |g: usize| -> &DKey {
        let t = base.partition_point(|&x| x <= g) - 1;
        kparts[t].get(g - base[t])
    };
    let mut g = 0usize;
    let mut any = false;
    for p in kparts.iter() {
        for sl in p.slices() {
            for rec in sl {
                let ek = rec.ekey();
                let h = hash128(ek);
                let fp = (h >> 20) as u32;
                let mut s = (h as usize) & mask;
                loop {
                    let cur = table[s];
                    if cur == u64::MAX {
                        table[s] = (g as u64) | ((fp as u64) << 32);
                        break;
                    }
                    if (cur >> 32) as u32 == fp {
                        let cg = cur as u32 as usize;
                        let o = get(cg);
                        if o.ekey() == ek {
                            any = true;
                            if rec.ingest < o.ingest {
                                table[s] = (g as u64) | ((fp as u64) << 32);
                                losers[cg / 64] |= 1 << (cg % 64);
                            } else {
                                losers[g / 64] |= 1 << (g % 64);
                            }
                            break;
                        }
                    }
                    s = (s + 1) & mask;
                }
                g += 1;
            }
        }
    }
    if !any {
        return 0;
    }
    let mut killed = 0;
    for (w, &bits) in losers.iter().enumerate() {
        let mut bits = bits;
        while bits != 0 {
            let g = w * 64 + bits.trailing_zeros() as usize;
            bits &= bits - 1;
            let loc = get(g).loc;
            if loc != NOLOC {
                let t = (loc >> 59) as usize;
                let pb = ((loc >> 40) & ((1 << 19) - 1)) as usize;
                let i = (loc & ((1 << 40) - 1)) as usize;
                let p = pay[t][pb].ptr(i);
                // distinct rows only, each marked once; read after the phase joins
                unsafe { std::sync::atomic::AtomicU8::from_ptr(std::ptr::addr_of_mut!((*p).kind)).store(K_INVALID, AO::Relaxed) };
                killed += 1;
            }
        }
    }
    killed
}

struct Scratch {
    starts: Vec<i64>,
    days: Vec<u32>,
    runs: Vec<(u32, u32, u32)>, // (day, first session, end session)
    ct: Vec<(u32, i64)>,
    catsum: Vec<i64>,
    touched: Vec<u16>,
    // grouping
    table: Vec<(u128, u32)>,
    pkeys: Vec<u128>,
    pid: Vec<u32>,
    eoff: Vec<u32>,
    coff: Vec<u32>,
    evs: Vec<EvOut>,
    cls: Vec<ClRec>,
    skeys: Vec<u64>,
    skeys2: Vec<u64>,
    tmp: Vec<EvOut>,
    pos: Vec<u32>,
}

/// LSD radix sort of u64 keys by their top 31 bits (3 passes of 11 bits).
fn radix_sort_hi31(a: &mut Vec<u64>, b: &mut Vec<u64>) {
    let n = a.len();
    b.clear();
    b.resize(n, 0);
    let mut cnt = [[0u32; 2048]; 3];
    for &k in a.iter() {
        cnt[0][((k >> 33) & 2047) as usize] += 1;
        cnt[1][((k >> 44) & 2047) as usize] += 1;
        cnt[2][((k >> 55) & 2047) as usize] += 1;
    }
    for c in cnt.iter_mut() {
        let mut s = 0u32;
        for x in c.iter_mut() {
            let v = *x;
            *x = s;
            s += v;
        }
    }
    for (pass, sh) in [33u32, 44, 55].into_iter().enumerate() {
        let (src, dst) = if pass % 2 == 0 { (&*a, &mut *b) } else { (&*b, &mut *a) };
        let c = &mut cnt[pass];
        for &k in src.iter() {
            let d = ((k >> sh) & 2047) as usize;
            dst[c[d] as usize] = k;
            c[d] += 1;
        }
    }
    std::mem::swap(a, b); // 3 passes: result is in b
}

impl Scratch {
    fn new(ncat: usize) -> Self {
        Scratch {
            starts: vec![],
            days: vec![],
            runs: vec![],
            ct: vec![],
            catsum: vec![0; ncat],
            touched: vec![],
            table: vec![],
            pkeys: vec![],
            pid: vec![],
            eoff: vec![],
            coff: vec![],
            evs: vec![],
            cls: vec![],
            skeys: vec![],
            skeys2: vec![],
            tmp: vec![],
            pos: vec![],
        }
    }
}

/// Groups the bucket's events and clicks by person (hash + counting sort),
/// sorts each person's rows by time, and processes them.
fn process_bucket(
    evsrc: &[&[EvOut]],
    clsrc: &[&[ClRec]],
    ctx: &Ctx,
    sc: &mut Scratch,
    rows: &mut Vec<URow>,
    camp: &mut [[i64; 4]],
) {
    let n: usize = evsrc.iter().map(|s| s.len()).sum(); // upper bound (includes dead rows)
    if n == 0 {
        return;
    }
    // ---- events: person ids via a local hash table, counted as we go ----
    let mut cap = ((n / 8).max(64)).next_power_of_two();
    'retry: loop {
        let mask = cap - 1;
        sc.table.clear();
        sc.table.resize(cap, (0, 0));
        sc.pkeys.clear();
        sc.pid.clear();
        sc.eoff.clear();
        sc.eoff.push(0);
        for sl in evsrc {
            for e in sl.iter() {
                if e.kind == K_INVALID {
                    continue;
                }
                let k = e.pkey;
                let mut i = (p_hash(k) as usize) & mask;
                let id = loop {
                    let s = unsafe { sc.table.get_unchecked_mut(i) };
                    if s.0 == k {
                        break s.1;
                    }
                    if s.0 == 0 {
                        if (sc.pkeys.len() + 1) * 2 > cap {
                            cap *= 4;
                            continue 'retry;
                        }
                        let id = sc.pkeys.len() as u32;
                        *s = (k, id);
                        sc.pkeys.push(k);
                        sc.eoff.push(0);
                        break id;
                    }
                    i = (i + 1) & mask;
                };
                unsafe { *sc.eoff.get_unchecked_mut(id as usize + 1) += 1 };
                sc.pid.push(id);
            }
        }
        break;
    }
    let np = sc.pkeys.len();
    let mask = cap - 1;
    for i in 0..np {
        sc.eoff[i + 1] += sc.eoff[i];
    }
    let nv = sc.eoff[np] as usize;
    // counting-sort scatter (every slot in 0..nv is written exactly once)
    sc.evs.clear();
    sc.evs.reserve(nv);
    sc.pos.clear();
    sc.pos.extend_from_slice(&sc.eoff[..np]);
    {
        let dst = sc.evs.as_mut_ptr();
        let mut j = 0usize;
        for sl in evsrc {
            for e in sl.iter() {
                if e.kind == K_INVALID {
                    continue;
                }
                unsafe {
                    let id = *sc.pid.get_unchecked(j) as usize;
                    let p = sc.pos.get_unchecked_mut(id);
                    dst.add(*p as usize).write(*e);
                    *p += 1;
                }
                j += 1;
            }
        }
        unsafe { sc.evs.set_len(nv) };
    }
    // ---- clicks of persons that have events ----
    sc.pid.clear();
    sc.coff.clear();
    sc.coff.resize(np + 1, 0);
    for sl in clsrc {
        for c in sl.iter() {
            let k = c.pkey;
            let mut i = (p_hash(k) as usize) & mask;
            let id = loop {
                let s = unsafe { sc.table.get_unchecked(i) };
                if s.0 == k {
                    break s.1;
                }
                if s.0 == 0 {
                    break u32::MAX;
                }
                i = (i + 1) & mask;
            };
            if id != u32::MAX {
                unsafe { *sc.coff.get_unchecked_mut(id as usize + 1) += 1 };
            }
            sc.pid.push(id);
        }
    }
    for i in 0..np {
        sc.coff[i + 1] += sc.coff[i];
    }
    let nc = sc.coff[np] as usize;
    sc.cls.clear();
    sc.cls.reserve(nc);
    sc.pos.clear();
    sc.pos.extend_from_slice(&sc.coff[..np]);
    {
        let dst = sc.cls.as_mut_ptr();
        let mut j = 0usize;
        for sl in clsrc {
            for c in sl.iter() {
                let id = unsafe { *sc.pid.get_unchecked(j) };
                j += 1;
                if id != u32::MAX {
                    unsafe {
                        let p = sc.pos.get_unchecked_mut(id as usize);
                        dst.add(*p as usize).write(*c);
                        *p += 1;
                    }
                }
            }
        }
        unsafe { sc.cls.set_len(nc) };
    }
    let evs_s = std::mem::take(&mut sc.evs);
    let mut evs_s = evs_s;
    let mut cls_s = std::mem::take(&mut sc.cls);
    for id in 0..np {
        let (ea, eb) = (sc.eoff[id] as usize, sc.eoff[id + 1] as usize);
        let (ca, cb) = (sc.coff[id] as usize, sc.coff[id + 1] as usize);
        let pe = &mut evs_s[ea..eb];
        if pe.len() > 24 {
            // sort compact u64 keys (time offset, index), then permute
            let ws = ctx.ws;
            sc.skeys.clear();
            sc.skeys.extend(pe.iter().enumerate().map(|(i, e)| (((e.ts - ws) as u64) << 33) | i as u64));
            if pe.len() > 2048 {
                radix_sort_hi31(&mut sc.skeys, &mut sc.skeys2);
            } else {
                sc.skeys.sort_unstable();
            }
            sc.tmp.clear();
            sc.tmp.extend(sc.skeys.iter().map(|&k| pe[(k & ((1 << 33) - 1)) as usize]));
            pe.copy_from_slice(&sc.tmp);
        } else if pe.len() > 1 {
            pe.sort_unstable_by_key(|e| e.ts);
        }
        let pc = &mut cls_s[ca..cb];
        if pc.len() > 1 {
            pc.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| cmp_key(a.ckey, b.ckey)));
        }
        let pk = sc.pkeys[id];
        process_person(pk, &evs_s[ea..eb], &cls_s[ca..cb], ctx, sc, rows, camp);
    }
    sc.evs = evs_s;
    sc.cls = cls_s;
}

#[allow(dead_code)]
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
    let (mut cj, mut crun) = (0usize, 0usize);
    let has_clicks = !cls.is_empty();

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
        // credited click: latest with ts <= start (sessions come in start
        // order, so the pointer only moves forward); ties -> smallest click_id,
        // which is the first of its equal-ts run since clicks are sorted by (ts, id)
        while cj < cls.len() && cls[cj].ts <= start {
            if cj == 0 || cls[cj].ts != cls[cj - 1].ts {
                crun = cj;
            }
            cj += 1;
        }
        let c = if cj > 0 && start - cls[cj - 1].ts <= WEEK_MS { cls[crun].camp as usize } else { organic };
        debug_assert_eq!(c, credit(cls, start, organic));
        let slot = &mut camp[c * ndays + day as usize];
        slot[0] += 1;
        slot[1] += purch as i64;
        slot[2] += rev;
        if has_clicks {
            sc.starts.push(start);
            sc.days.push(day);
        }
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
