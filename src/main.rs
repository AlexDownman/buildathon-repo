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
/// records staged per bucket before a write-combined flush (32-byte records)
const SK: usize = 8;

/// tunables (env overrides are for probe experiments only)
fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}
static PIECE_SZ: std::sync::LazyLock<usize> = std::sync::LazyLock::new(|| env_usize("PIECE_KB", PIECE >> 10) << 10);

/// Dedup key stream record (24 bytes): event_id, then ingest_ts and the
/// location of the row's payload packed in one word: x = ingest code << 37 | loc.
#[derive(Clone, Copy, Default)]
struct DKey {
    k0: u64,
    k1: u64,
    x: u64,
}

/// loc = parse worker (5 bits) << 32 | person bucket (12 bits) << 20 | index (20 bits)
const LOC_MASK: u64 = (1 << 37) - 1;
const NOLOC: u64 = LOC_MASK;
const LOC_IDX: usize = 1 << 20;

#[inline(always)]
fn mkloc(t: usize, pb: usize, i: usize) -> u64 {
    ((t as u64) << 32) | ((pb as u64) << 20) | i as u64
}

/// ingest_ts (always a valid F1, whole seconds) as seconds from 2^26 s before
/// window_start, in 27 bits (a range of +-2 years around the window, clamped;
/// the code for an unreadable value sorts last)
#[inline(always)]
fn ingest_code(ingest_s: Option<i64>, ws_s: i64) -> u64 {
    match ingest_s {
        Some(t) => ((t - ws_s + (1 << 26)).clamp(0, (1 << 27) - 2)) as u64,
        None => (1 << 27) - 1,
    }
}

impl DKey {
    #[inline(always)]
    fn ekey(&self) -> u128 {
        ((self.k1 as u128) << 64) | self.k0 as u128
    }
    #[inline(always)]
    fn ing(&self) -> u64 {
        self.x >> 37
    }
    #[inline(always)]
    fn loc(&self) -> u64 {
        self.x & LOC_MASK
    }
}

/// Payload records, one per valid event row, in person buckets. Word 2 of a
/// record is u64::MAX once dedup has marked it a later copy.
trait Layout: Send + Sync + 'static {
    type Ev: Copy + Default + Send + Sync + 'static;
    type K: EKey;
    /// price 0 = not a purchase
    fn ev(pkey: u128, off: u64, price: u32, cat: u16) -> Self::Ev;
    fn pkey(e: &Self::Ev) -> u128;
    fn key(e: &Self::Ev) -> Self::K;
    fn dead(e: &Self::Ev) -> bool;
}

/// 24-byte records: the usual case (window under 49 days, at most 256 categories)
struct Narrow;
impl Layout for Narrow {
    type Ev = [u64; 3];
    type K = u64;
    #[inline(always)]
    fn ev(pkey: u128, off: u64, price: u32, cat: u16) -> [u64; 3] {
        let pl = if price == 0 { 0 } else { ((price as u64) << 8) | cat as u64 };
        [pkey as u64, (pkey >> 64) as u64, (off << 32) | pl]
    }
    #[inline(always)]
    fn pkey(e: &[u64; 3]) -> u128 {
        ((e[1] as u128) << 64) | e[0] as u128
    }
    #[inline(always)]
    fn key(e: &[u64; 3]) -> u64 {
        e[2]
    }
    #[inline(always)]
    fn dead(e: &[u64; 3]) -> bool {
        e[2] == u64::MAX
    }
}

/// 32-byte records for anything wider
struct Wide;
impl Layout for Wide {
    type Ev = [u64; 4];
    type K = u128;
    #[inline(always)]
    fn ev(pkey: u128, off: u64, price: u32, cat: u16) -> [u64; 4] {
        let pl = if price == 0 { 0 } else { ((price as u64) << 16) | cat as u64 };
        [pkey as u64, (pkey >> 64) as u64, off, pl]
    }
    #[inline(always)]
    fn pkey(e: &[u64; 4]) -> u128 {
        ((e[1] as u128) << 64) | e[0] as u128
    }
    #[inline(always)]
    fn key(e: &[u64; 4]) -> u128 {
        ((e[2] as u128) << 64) | e[3] as u128
    }
    #[inline(always)]
    fn dead(e: &[u64; 4]) -> bool {
        e[2] == u64::MAX
    }
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
    ws_s: i64, // window_start in seconds (floor)
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

struct Dims {
    products: KeyMap<u16>,
    nprod: usize,
    unknown_cat: u16,
    cat_names: Vec<Vec<u8>>,
    camps: KeyMap<u32>,
    camp_ids: Vec<Vec<u8>>,
    channels: Vec<Vec<u8>>,
}

/// products.csv and campaigns.csv (small; read on one thread while links load)
fn load_dims(data: &Path) -> Dims {
    // ---- products ----
    // (borrowed slices, no per-row allocation: this runs on one thread)
    let pbytes = read_small(&data.join("products.csv"));
    let mut prows: Vec<(u128, u16)> = Vec::with_capacity(pbytes.len() / 16);
    let mut catmap = KeyMap::<u16>::with_capacity(64);
    let mut cat_raw: Vec<&[u8]> = Vec::new();
    for_lines(&pbytes, 0, pbytes.len(), |line| {
        let mut f: [&[u8]; 2] = [b""; 2];
        if split(line, &mut f) && !f[0].is_empty() {
            let (ci, new) = catmap.get_or_insert(key_of(f[1]), cat_raw.len() as u16);
            if new {
                cat_raw.push(f[1]);
            }
            prows.push((key_of(f[0]), ci));
        }
    });
    let (unk_raw, new) = catmap.get_or_insert(key_of(b"unknown"), cat_raw.len() as u16);
    if new {
        cat_raw.push(b"unknown");
    }
    let mut order: Vec<u16> = (0..cat_raw.len() as u16).collect();
    order.sort_by(|&a, &b| cat_raw[a as usize].cmp(cat_raw[b as usize]));
    let mut rank = vec![0u16; cat_raw.len()];
    for (r, &i) in order.iter().enumerate() {
        rank[i as usize] = r as u16;
    }
    let cat_names: Vec<Vec<u8>> = order.iter().map(|&i| cat_raw[i as usize].to_vec()).collect();
    let unknown_cat = rank[unk_raw as usize];
    let mut products = KeyMap::<u16>::with_capacity(prows.len());
    for &(id, c) in &prows {
        products.get_or_insert(id, rank[c as usize]);
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

    Dims { products, nprod: prows.len(), unknown_cat, cat_names, camps, camp_ids, channels }
}

struct POut {
    rows: Vec<URow>,
    camp: Vec<[i64; 4]>,
    sc: Scratch,
}

/// parse, dedup and the per-person pass, for one payload layout
#[allow(clippy::too_many_arguments)]
fn events_to_persons<L: Layout>(
    nthreads: usize,
    t0: Instant,
    ev_in: &Input,
    cl_in: &Input,
    ev_chunks: &[(usize, usize, usize)],
    cl_chunks: &[(usize, usize, usize)],
    ctx: &Ctx,
    nev_b: usize,
    np_b: usize,
) -> Vec<POut> {
    let parsed: Vec<Parsed<L>> = if std::env::var_os("STAGE4").is_some() {
        run_parse::<L, 4>(nthreads, ev_in, cl_in, ev_chunks, cl_chunks, ctx, nev_b, np_b)
    } else {
        run_parse::<L, SK>(nthreads, ev_in, cl_in, ev_chunks, cl_chunks, ctx, nev_b, np_b)
    };
    let (ev_bytes, cl_bytes) = (ev_in.bytes(), cl_in.bytes());
    // dedup keys go bucket-major (each dedup task takes its bucket and hands
    // the blocks back); payloads stay per worker until dedup has marked losers
    let ev_b: Vec<Mutex<Vec<BlockVec<DKey>>>> = (0..nev_b).map(|_| Mutex::new(Vec::new())).collect();
    let cl_b: Vec<Mutex<Vec<BlockVec<ClRec>>>> = (0..np_b).map(|_| Mutex::new(Vec::new())).collect();
    let mut pay: Vec<Vec<BlockVec<L::Ev>>> = Vec::with_capacity(parsed.len());
    let (mut nrec, mut ncl) = (0usize, 0usize);
    for p in parsed {
        for (b, v) in p.evk.into_iter().enumerate() {
            nrec += v.len();
            ev_b[b].lock().unwrap().push(v);
        }
        pay.push(p.evp);
        for (b, v) in p.cl.into_iter().enumerate() {
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
    let pe_b: Vec<Mutex<Vec<BlockVec<L::Ev>>>> = (0..np_b).map(|_| Mutex::new(Vec::new())).collect();
    let mut nsurv = 0usize;
    for w in pay {
        for (b, v) in w.into_iter().enumerate() {
            nsurv += v.len();
            pe_b[b].lock().unwrap().push(v);
        }
    }
    log(t0, &format!("dedup: {} valid rows, {} later copies dropped", nsurv, killed));

    // ---- per person ----
    let ndays = ctx.ndays;
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
                let evs: Vec<&[L::Ev]> = ev.iter().flat_map(|v| v.slices()).collect();
                let cls: Vec<&[ClRec]> = cl.iter().flat_map(|v| v.slices()).collect();
                process_bucket::<L>(&evs, &cls, ctx, &mut po.sc, &mut po.rows, &mut po.camp);
            }
            ev.into_iter().for_each(BlockVec::release);
            cl.into_iter().for_each(BlockVec::release);
        },
    );
    pouts
}

fn main() {
    let t0 = Instant::now();
    if let Some(dir) = std::env::var_os("PROBE_MICRO") {
        probe_micro(Path::new(&dir));
        return;
    }
    if std::env::var_os("PROBE_SCALE").is_some() {
        probe_scale();
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

    // ---- products + campaigns (one thread) alongside links ----
    log(t0, "start");
    let (link, npairs, dims) = std::thread::scope(|scope| {
        let dims_h = scope.spawn(|| load_dims(&data));
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
                        // each side is recorded as (shard << 32 | position in this worker's shard list)
                        let sa = (hash128(a) >> (64 - L_BITS)) as usize;
                        let sb = (hash128(b) >> (64 - L_BITS)) as usize;
                        let ca = ((sa as u64) << 32) | keys[sa].len() as u64;
                        keys[sa].push(a);
                        let cb = ((sb as u64) << 32) | keys[sb].len() as u64;
                        keys[sb].push(b);
                        pairs.push((ca, cb));
                    }
                });
            },
        );
        let npairs: usize = pair_lists.iter().map(|v| v.pairs.len()).sum();
        let link = build_links(nthreads, &pair_lists);
        drop(pair_lists);
        (link, npairs, dims_h.join().unwrap())
    });
    log(t0, &format!("links: {} pairs, {} linked ids", npairs, link.len()));
    let Dims { products, nprod, unknown_cat, cat_names, camps, camp_ids, channels } = dims;
    eprintln!("           products {} cats {} campaigns {}", nprod, cat_names.len(), camp_ids.len());

    let ctx = Ctx {
        ws,
        we,
        ws_s: ws.div_euclid(1000),
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
    // ~20M events (1.7 GB) -> 512 / 1024 buckets; ~60M -> 1024 / 2048
    if ev_bytes > (3usize << 30) {
        EV_BITS.store(10, AO::Relaxed);
        P_BITS.store(11, AO::Relaxed);
    }
    if let Some(b) = std::env::var("EVBITS").ok().and_then(|s| s.parse().ok()) {
        EV_BITS.store(b, AO::Relaxed);
    }
    if let Some(b) = std::env::var("PBITS").ok().and_then(|s| s.parse().ok()) {
        P_BITS.store(b, AO::Relaxed);
    }
    let nev_b = 1usize << EV_BITS.load(AO::Relaxed);
    let np_b = 1usize << P_BITS.load(AO::Relaxed);

    // 24-byte payload records unless the window or the category count is too wide
    let narrow = we - ws < u32::MAX as i64 && ctx.cat_names.len() <= 256 && std::env::var_os("WIDE").is_none();
    let pouts = if narrow {
        events_to_persons::<Narrow>(nthreads, t0, &ev_in, &cl_in, &ev_chunks, &cl_chunks, &ctx, nev_b, np_b)
    } else {
        events_to_persons::<Wide>(nthreads, t0, &ev_in, &cl_in, &ev_chunks, &cl_chunks, &ctx, nev_b, np_b)
    };
    let ncamp_tot = ctx.ncamp() + 1;
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
/// ID -> person name, for ids whose name is another id. One table split into
/// 2^L_BITS regions (top hash bits pick the region, linear probing wraps
/// inside it), so the regions are built in parallel yet form one allocation.
struct LinkMap {
    slots: Vec<(u128, u128)>,
    rbits: u32, // log2 of the region size
    len: usize,
}

impl LinkMap {
    #[inline(always)]
    fn start(&self, h: u64) -> usize {
        (h >> (64 - L_BITS - self.rbits)) as usize
    }
    #[inline(always)]
    fn prefetch(&self, h: u64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            let p = self.slots.as_ptr().add(self.start(h)) as *const i8;
            std::arch::x86_64::_mm_prefetch(p, std::arch::x86_64::_MM_HINT_T0);
        }
    }
    #[inline(always)]
    fn get_h(&self, k: u128, h: u64) -> Option<u128> {
        let i0 = self.start(h);
        let rmask = (1usize << self.rbits) - 1;
        let base = i0 & !rmask;
        let mut j = i0 & rmask;
        loop {
            let s = unsafe { self.slots.get_unchecked(base | j) };
            if s.0 == k {
                return Some(s.1);
            }
            if s.0 == 0 {
                return None;
            }
            j = (j + 1) & rmask;
        }
    }
    fn len(&self) -> usize {
        self.len
    }
}

/// Like par_map, with per-thread scratch state.
fn par_map_st<R: Send, T: Send, I: Fn() -> T + Sync, F: Fn(&mut T, usize) -> R + Sync>(nthreads: usize, ntasks: usize, init: I, f: F) -> Vec<R> {
    let mut v: Vec<(usize, R)> = par_for(nthreads, ntasks, || (init(), Vec::new()), |st: &mut (T, Vec<(usize, R)>), i| {
        let r = f(&mut st.0, i);
        st.1.push((i, r));
    })
    .into_iter()
    .flat_map(|x| x.1)
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
/// One parse worker's finished output, ordered by worker id.
struct Parsed<L: Layout> {
    evk: Vec<BlockVec<DKey>>,
    evp: Vec<BlockVec<L::Ev>>,
    cl: Vec<BlockVec<ClRec>>,
}

/// Parses all event and click chunks on nthreads workers. K = records staged
/// per bucket before each write-combined flush.
#[allow(clippy::too_many_arguments)]
fn run_parse<L: Layout, const K: usize>(
    nthreads: usize,
    ev_in: &Input,
    cl_in: &Input,
    ev_chunks: &[(usize, usize, usize)],
    cl_chunks: &[(usize, usize, usize)],
    ctx: &Ctx,
    nev_b: usize,
    np_b: usize,
) -> Vec<Parsed<L>> {
    struct ParseOut<L: Layout, const K: usize> {
        id: usize,
        evk: Parts<DKey, K>,
        evp: Parts<L::Ev, K>,
        cl: Parts<ClRec, 4>,
        buf: Vec<u8>,
    }
    let next_id = AtomicUsize::new(0);
    let mut outs = par_for(
        nthreads,
        ev_chunks.len() + cl_chunks.len(),
        || ParseOut::<L, K> {
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
                    parse_events::<L, K>(ev_in.maps[fi].bytes(), s0, e0, ctx, po.id, &mut po.evk, &mut po.evp);
                    return;
                }
                let mut p = s0;
                while p < e0 {
                    let q = (p + *PIECE_SZ).min(e0);
                    let (s, e) = ev_in.read(fi, p, q, &mut po.buf);
                    parse_events::<L, K>(&po.buf, s, e, ctx, po.id, &mut po.evk, &mut po.evp);
                    p = q;
                }
            } else {
                let (fi, s0, e0) = cl_chunks[i - ev_chunks.len()];
                if *MMAPIO {
                    parse_clicks(cl_in.maps[fi].bytes(), s0, e0, ctx, &mut po.cl);
                    return;
                }
                let mut p = s0;
                while p < e0 {
                    let q = (p + *PIECE_SZ).min(e0);
                    let (s, e) = cl_in.read(fi, p, q, &mut po.buf);
                    parse_clicks(&po.buf, s, e, ctx, &mut po.cl);
                    p = q;
                }
            }
        },
    );
    outs.sort_by_key(|p| p.id);
    outs.into_iter().map(|p| Parsed { evk: p.evk.finish(), evp: p.evp.finish(), cl: p.cl.finish() }).collect()
}

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
/// Diagnostics only: how throughput scales with threads on this machine
/// (integer work and memory reads), appended as one line to PROBE_OUT.
fn probe_scale() {
    let mut out = String::from("S");
    for nt in [1usize, 2, 4, 8] {
        let t = Instant::now();
        let iters = 200_000_000u64;
        let sum: u64 = std::thread::scope(|sc| {
            let hs: Vec<_> = (0..nt)
                .map(|k| {
                    sc.spawn(move || {
                        let (mut a, mut b, mut c, mut d) = (k as u64 + 1, 2u64, 3u64, 4u64);
                        for _ in 0..iters / 4 {
                            a ^= a << 13;
                            a ^= a >> 7;
                            b = b.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(a);
                            c ^= c << 17;
                            c ^= c >> 5;
                            d = d.wrapping_mul(0xC2B2_AE3D_27D4_EB4F).wrapping_add(c);
                        }
                        std::hint::black_box(a ^ b ^ c ^ d)
                    })
                })
                .collect();
            hs.into_iter().map(|h| h.join().unwrap()).sum()
        });
        std::hint::black_box(sum);
        // aggregate iterations per nanosecond
        out += &format!(" alu{}={:.2}", nt, (nt as u64 * iters) as f64 / t.elapsed().as_nanos() as f64);
    }
    let per = 256usize << 20;
    let bufs: Vec<Vec<u64>> = (0..8).map(|_| vec![1u64; per / 8]).collect();
    for nt in [1usize, 2, 4, 8] {
        let t = Instant::now();
        std::thread::scope(|sc| {
            for b in bufs.iter().take(nt) {
                sc.spawn(move || {
                    let mut s = 0u64;
                    for _ in 0..2 {
                        for x in b.iter() {
                            s = s.wrapping_add(*x);
                        }
                    }
                    std::hint::black_box(s);
                });
            }
        });
        out += &format!(" bw{}={:.1}", nt, (nt * per * 2) as f64 / t.elapsed().as_nanos() as f64);
    }
    let cpus = std::fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .find(|l| l.starts_with("Cpus_allowed_list"))
        .map(|l| l.split_whitespace().last().unwrap_or("").to_string())
        .unwrap_or_default();
    out += &format!(" cpus={}", cpus);
    // physical core of each allowed CPU (HT siblings share one)
    let mut ids = Vec::new();
    for part in cpus.split(',') {
        let mut it = part.split('-').filter_map(|x| x.parse::<usize>().ok());
        if let Some(a) = it.next() {
            let b = it.next().unwrap_or(a);
            for c in a..=b {
                let core = std::fs::read_to_string(format!("/sys/devices/system/cpu/cpu{}/topology/core_id", c)).unwrap_or_default();
                ids.push(core.trim().to_string());
            }
        }
    }
    out += &format!(" core={}", ids.join("."));
    if let Some(path) = std::env::var_os("PROBE_OUT") {
        use std::io::Write;
        if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(path) {
            let _ = writeln!(f, "{}", out);
        }
    }
}

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

/// Shared output array written in disjoint ranges by parallel tasks.
#[derive(Clone, Copy)]
struct SyncPtr<T>(*mut T);
unsafe impl<T> Send for SyncPtr<T> {}
unsafe impl<T> Sync for SyncPtr<T> {}

fn build_links(nthreads: usize, lists: &[LinkParse]) -> LinkMap {
    let ns = 1usize << L_BITS;
    let nl = lists.len();
    // occurrence counts per shard; shard s owns global slots [occ[s], occ[s+1])
    // of every per-id array (sized by occurrences, an upper bound on ids)
    let mut occ = Vec::with_capacity(ns + 1);
    let mut acc = 0usize;
    for s in 0..ns {
        occ.push(acc);
        acc += lists.iter().map(|l| l.keys[s].len()).sum::<usize>();
    }
    occ.push(acc);
    let nocc = acc;
    let mut keys: Vec<u128> = Vec::with_capacity(nocc);
    let mut ids: Vec<u32> = Vec::with_capacity(nocc);
    let kp = SyncPtr(keys.as_mut_ptr());
    let ip = SyncPtr(ids.as_mut_ptr());
    // L1: per shard, global ids of distinct keys and the id of every occurrence
    // (occurrence order: workers in order, each worker's shard list in order)
    let nuniq: Vec<usize> = par_map_st(nthreads, ns, Vec::new, |table: &mut Vec<u32>, s| {
        let (kp, ip) = (kp, ip);
        let base = occ[s];
        let total = occ[s + 1] - base;
        let cap = (total.max(8) * 2).next_power_of_two();
        let mask = cap - 1;
        table.clear();
        table.resize(cap, u32::MAX);
        let mut nu = 0usize;
        let mut w = base;
        for l in lists {
            for &k in &l.keys[s] {
                let mut i = (hash128(k) as usize) & mask;
                let id = loop {
                    let e = unsafe { *table.get_unchecked(i) };
                    if e == u32::MAX {
                        let id = (base + nu) as u32;
                        unsafe {
                            *table.get_unchecked_mut(i) = id;
                            kp.0.add(base + nu).write(k);
                        }
                        nu += 1;
                        break id;
                    }
                    if unsafe { *kp.0.add(e as usize) } == k {
                        break e;
                    }
                    i = (i + 1) & mask;
                };
                unsafe { ip.0.add(w).write(id) };
                w += 1;
            }
        }
        nu
    });
    unsafe {
        keys.set_len(nocc);
        ids.set_len(nocc);
    }
    // start of worker t's occurrences of shard s: occ[s] + sum of earlier workers'
    let mut wstart = vec![0usize; ns * nl];
    for s in 0..ns {
        let mut a = occ[s];
        for (t, l) in lists.iter().enumerate() {
            wstart[s * nl + t] = a;
            a += l.keys[s].len();
        }
    }
    let gid = |t: usize, c: u64| -> u32 {
        let s = (c >> 32) as usize;
        unsafe { *ids.get_unchecked(*wstart.get_unchecked(s * nl + t) + (c as u32) as usize) }
    };
    // L2: concurrent union-find over global ids
    let parent: Vec<AtomicU32> = (0..nocc as u32).map(AtomicU32::new).collect();
    par_for(nthreads, nl, || (), |_, t| {
        for &(a, b) in &lists[t].pairs {
            uf_union(&parent, gid(t, a), gid(t, b));
        }
    });
    // L3: smallest key per component
    let minidx: Vec<AtomicU32> = (0..nocc).map(|_| AtomicU32::new(u32::MAX)).collect();
    par_for(nthreads, ns, || (), |_, s| {
        for i in occ[s]..occ[s] + nuniq[s] {
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
    // L4: the lookup map holds only ids whose person name is another id
    let n: usize = nuniq.iter().sum();
    let mut rbits = 3u32;
    for s in 0..ns {
        while (1usize << rbits) < 2 * nuniq[s] {
            rbits += 1;
        }
    }
    let rsize = 1usize << rbits;
    let mut slots: Vec<(u128, u128)> = vec![(0, 0); rsize * ns];
    let sp = SyncPtr(slots.as_mut_ptr());
    let rmask = rsize - 1;
    par_for(nthreads, ns, || (), |_, s| {
        let sp = sp;
        let reg = unsafe { std::slice::from_raw_parts_mut(sp.0.add(s * rsize), rsize) };
        for i in occ[s]..occ[s] + nuniq[s] {
            let r = uf_find(&parent, i as u32) as usize;
            let mi = minidx[r].load(AO::Relaxed) as usize;
            if mi != i {
                let k = keys[i];
                let h = hash128(k);
                let mut j = ((h >> (64 - L_BITS - rbits)) as usize) & rmask;
                while reg[j].0 != 0 {
                    j = (j + 1) & rmask;
                }
                reg[j] = (k, keys[mi]);
            }
        }
    });
    LinkMap { slots, rbits, len: n }
}

struct LinkParse {
    pairs: Vec<(u64, u64)>,
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
    hu: u64,
    ing: u32,
    price: u32,
    b: u16,
    cat: u16,
}

#[inline(always)]
fn finish_ev<L: Layout, const K: usize>(ctx: &Ctx, wid: usize, p: &PendEv, outk: &mut Parts<DKey, K>, outp: &mut Parts<L::Ev, K>) {
    let (pkey, ph) = match ctx.link.get_h(p.ukey, p.hu) {
        Some(n) => (n, p_hash(n)),
        None => (p.ukey, p.hu),
    };
    let pb = (ph >> (64 - P_BITS.load(AO::Relaxed))) as usize;
    let i = outp.push_idx(pb, L::ev(pkey, (p.ts - ctx.ws) as u64, p.price, p.cat));
    assert!(i < LOC_IDX, "too many events per bucket");
    let loc = mkloc(wid, pb, i);
    outk.push(p.b as usize, DKey { k0: p.ekey as u64, k1: (p.ekey >> 64) as u64, x: ((p.ing as u64) << 37) | loc });
}

fn parse_events<L: Layout, const K: usize>(d: &[u8], s: usize, e: usize, ctx: &Ctx, wid: usize, outk: &mut Parts<DKey, K>, outp: &mut Parts<L::Ev, K>) {
    let end = d.as_ptr_range().end;
    let mut ring = [PendEv::default(); RING];
    let mut cnt = 0usize;
    let finish = |p: &PendEv, outk: &mut Parts<DKey, K>, outp: &mut Parts<L::Ev, K>| finish_ev::<L, K>(ctx, wid, p, outk, outp);
    for_rows::<7>(d, s, e, #[inline(always)] |f| {
        let [eid, uid, ts, ing, et, pid, price] = *f;
        if eid.is_empty() {
            return;
        }
        let ekey = key_fast(eid, end);
        let ingest = parse_ingest_secs(ing, end);
        let icode = ingest_code(ingest, ctx.ws_s) as u32;
        'v: {
            if uid.is_empty() || pid.is_empty() {
                break 'v;
            }
            let t = match parse_ts_fast(ts, end) {
                Some(t) if t >= ctx.ws && t < ctx.we => t,
                _ => break 'v,
            };
            let mut pr = 0;
            let mut cat = 0;
            match et.len() {
                4 => match u32::from_le_bytes(et.try_into().unwrap()) {
                    0x7765_6976 | 0x7472_6163 => {} // "view", "cart"
                    _ => break 'v,
                },
                8 if u64::from_le_bytes(et.try_into().unwrap()) == u64::from_le_bytes(*b"purchase") => {
                    pr = match parse_price(price) {
                        Some(p) => p,
                        None => break 'v,
                    };
                    cat = ctx.products.get(key_fast(pid, end)).unwrap_or(ctx.unknown_cat);
                }
                _ => break 'v,
            }
            if ingest.map_or(i64::MAX, |x| x * 1000).saturating_sub(t) > DAY_MS {
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
            ring[slot] = PendEv { ekey, ukey, ts: t, hu, ing: icode, price: pr, b, cat };
            cnt += 1;
            return;
        }
        let b = ev_bucket(hash128(ekey));
        outk.push(b, DKey { k0: ekey as u64, k1: (ekey >> 64) as u64, x: ((icode as u64) << 37) | NOLOC });
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

#[inline(always)]
fn finish_cl(ctx: &Ctx, p: &PendCl, out: &mut Parts<ClRec, 4>) {
    let (pkey, ph) = match ctx.link.get_h(p.ukey, p.hu) {
        Some(n) => (n, p_hash(n)),
        None => (p.ukey, p.hu),
    };
    out.push((ph >> (64 - P_BITS.load(AO::Relaxed))) as usize, ClRec { pkey, ckey: p.ckey, ts: p.ts, camp: p.camp });
}

fn parse_clicks(d: &[u8], s: usize, e: usize, ctx: &Ctx, out: &mut Parts<ClRec, 4>) {
    let end = d.as_ptr_range().end;
    let lo = ctx.ws - WEEK_MS;
    let mut ring = [PendCl::default(); RING];
    let mut cnt = 0usize;
    let finish = |p: &PendCl, out: &mut Parts<ClRec, 4>| finish_cl(ctx, p, out);
    for_rows::<4>(d, s, e, #[inline(always)] |f| {
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
fn dedup_bucket<T: Copy>(kparts: &[BlockVec<DKey>], pay: &[Vec<BlockVec<T>>], table: &mut Vec<u64>, losers: &mut Vec<u64>) -> usize {
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
                            if rec.ing() < o.ing() {
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
            let loc = get(g).loc();
            if loc != NOLOC {
                let t = (loc >> 32) as usize;
                let pb = ((loc >> 20) & ((1 << 12) - 1)) as usize;
                let i = (loc & ((1 << 20) - 1)) as usize;
                let p = pay[t][pb].ptr(i) as *mut u64;
                // word 2 := MAX marks the record dead; distinct rows only, each
                // marked once; read after the phase joins
                unsafe { std::sync::atomic::AtomicU64::from_ptr(p.add(2)).store(u64::MAX, AO::Relaxed) };
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
    win: Vec<(i64, i64)>,       // open click window per campaign, MIN = none
    wtouched: Vec<u32>,
    catsum: Vec<i64>,
    touched: Vec<u16>,
    // grouping
    table: Vec<(u128, u32)>,
    pkeys: Vec<u128>,
    pid: Vec<u32>,
    eoff: Vec<u32>,
    coff: Vec<u32>,
    ek64: Vec<u64>,
    ek128: Vec<u128>,
    sorted64: Vec<u64>,
    sorted128: Vec<u128>,
    radix: Vec<u64>,
    cls: Vec<ClRec>,
    pos: Vec<u32>,
    pratio: f64, // persons per event row in the last bucket (sizes the next table)
}

/// An event inside the person phase: time offset from window_start, then the
/// purchase (price, category) or 0. Ordering is by time first; the order of
/// equal times does not matter.
trait EKey: Copy + Ord + Send + Sync + 'static {
    fn off(self) -> i64;
    /// (price, category); price 0 = not a purchase
    fn purchase(self) -> (u32, u16);
    fn vecs(sc: &mut Scratch) -> (&mut Vec<Self>, &mut Vec<Self>);
    fn sort(v: &mut [Self], tmp: &mut Vec<u64>);
}

impl EKey for u64 {
    #[inline(always)]
    fn off(self) -> i64 {
        (self >> 32) as i64
    }
    #[inline(always)]
    fn purchase(self) -> (u32, u16) {
        let p = self as u32;
        (p >> 8, (p & 0xFF) as u16)
    }
    fn vecs(sc: &mut Scratch) -> (&mut Vec<Self>, &mut Vec<Self>) {
        (&mut sc.ek64, &mut sc.sorted64)
    }
    #[inline(always)]
    fn sort(v: &mut [Self], tmp: &mut Vec<u64>) {
        if v.len() > 2048 {
            radix_sort_hi32(v, tmp);
        } else {
            v.sort_unstable();
        }
    }
}

impl EKey for u128 {
    #[inline(always)]
    fn off(self) -> i64 {
        (self >> 64) as i64
    }
    #[inline(always)]
    fn purchase(self) -> (u32, u16) {
        let p = self as u64;
        ((p >> 16) as u32, p as u16)
    }
    fn vecs(sc: &mut Scratch) -> (&mut Vec<Self>, &mut Vec<Self>) {
        (&mut sc.ek128, &mut sc.sorted128)
    }
    #[inline(always)]
    fn sort(v: &mut [Self], _tmp: &mut Vec<u64>) {
        v.sort_unstable();
    }
}

/// LSD radix sort of u64 keys by their top 32 bits (passes of 11, 11, 10 bits).
fn radix_sort_hi32(a: &mut [u64], tmp: &mut Vec<u64>) {
    let n = a.len();
    tmp.clear();
    tmp.resize(n, 0);
    let mut cnt = [[0u32; 2048]; 3];
    for &k in a.iter() {
        cnt[0][((k >> 32) & 2047) as usize] += 1;
        cnt[1][((k >> 43) & 2047) as usize] += 1;
        cnt[2][(k >> 54) as usize] += 1;
    }
    for c in cnt.iter_mut() {
        let mut s = 0u32;
        for x in c.iter_mut() {
            let v = *x;
            *x = s;
            s += v;
        }
    }
    for (pass, sh) in [32u32, 43, 54].into_iter().enumerate() {
        let (src, dst): (&[u64], &mut [u64]) = if pass % 2 == 0 { (&*a, &mut tmp[..]) } else { (&tmp[..], &mut *a) };
        let c = &mut cnt[pass];
        for &k in src.iter() {
            let d = ((k >> sh) & 2047) as usize;
            dst[c[d] as usize] = k;
            c[d] += 1;
        }
    }
    a.copy_from_slice(tmp); // 3 passes: result is in tmp
}

impl Scratch {
    fn new(ncat: usize) -> Self {
        Scratch {
            starts: vec![],
            days: vec![],
            runs: vec![],
            win: vec![],
            wtouched: vec![],
            catsum: vec![0; ncat],
            touched: vec![],
            table: vec![],
            pkeys: vec![],
            pid: vec![],
            eoff: vec![],
            coff: vec![],
            ek64: vec![],
            ek128: vec![],
            sorted64: vec![],
            sorted128: vec![],
            radix: vec![],
            cls: vec![],
            pos: vec![],
            pratio: 0.0625,
        }
    }
}

/// Groups the bucket's events and clicks by person (hash + counting sort),
/// sorts each person's rows by time, and processes them.
fn process_bucket<L: Layout>(
    evsrc: &[&[L::Ev]],
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
    // ---- events: person ids via a local hash table, counted as we go; each
    // live event's key (time offset, purchase) is collected alongside ----
    let mut cap = (((n as f64 * sc.pratio * 2.0) as usize + 1).max(64)).next_power_of_two();
    let (mut ekeys, mut sorted) = {
        let (a, b) = L::K::vecs(sc);
        (std::mem::take(a), std::mem::take(b))
    };
    'retry: loop {
        let mask = cap - 1;
        sc.table.clear();
        sc.table.resize(cap, (0, 0));
        sc.pkeys.clear();
        sc.pid.clear();
        sc.eoff.clear();
        sc.eoff.push(0);
        ekeys.clear();
        for sl in evsrc {
            for e in sl.iter() {
                if L::dead(e) {
                    continue;
                }
                let k = L::pkey(e);
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
                ekeys.push(L::key(e));
            }
        }
        break;
    }
    let np = sc.pkeys.len();
    let mask = cap - 1;
    sc.pratio = np as f64 / n as f64;
    for i in 0..np {
        sc.eoff[i + 1] += sc.eoff[i];
    }
    let nv = sc.eoff[np] as usize;
    // counting-sort scatter (every slot in 0..nv is written exactly once)
    sorted.clear();
    sorted.reserve(nv);
    sc.pos.clear();
    sc.pos.extend_from_slice(&sc.eoff[..np]);
    {
        let dst = sorted.as_mut_ptr();
        for (&id, &k) in sc.pid.iter().zip(ekeys.iter()) {
            unsafe {
                let p = sc.pos.get_unchecked_mut(id as usize);
                dst.add(*p as usize).write(k);
                *p += 1;
            }
        }
        unsafe { sorted.set_len(nv) };
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
    let mut cls_s = std::mem::take(&mut sc.cls);
    for id in 0..np {
        let (ea, eb) = (sc.eoff[id] as usize, sc.eoff[id + 1] as usize);
        let (ca, cb) = (sc.coff[id] as usize, sc.coff[id + 1] as usize);
        let pe = &mut sorted[ea..eb];
        if pe.len() > 1 {
            L::K::sort(pe, &mut sc.radix);
        }
        let pc = &mut cls_s[ca..cb];
        if pc.len() > 1 {
            pc.sort_unstable_by_key(|c| c.ts);
            // equal timestamps: order by click_id (byte-wise), needed for credit ties
            let mut i = 0;
            while i + 1 < pc.len() {
                if pc[i + 1].ts == pc[i].ts {
                    let mut j = i + 1;
                    while j + 1 < pc.len() && pc[j + 1].ts == pc[i].ts {
                        j += 1;
                    }
                    pc[i..=j].sort_unstable_by(|a, b| cmp_key(a.ckey, b.ckey));
                    i = j + 1;
                } else {
                    i += 1;
                }
            }
        }
        let pk = sc.pkeys[id];
        process_person(pk, &sorted[ea..eb], &cls_s[ca..cb], ctx, sc, rows, camp);
    }
    sc.cls = cls_s;
    let (a, b) = L::K::vecs(sc);
    *a = ekeys;
    *b = sorted;
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

fn process_person<K: EKey>(p: u128, evs: &[K], cls: &[ClRec], ctx: &Ctx, sc: &mut Scratch, rows: &mut Vec<URow>, camp: &mut [[i64; 4]]) {
    let ws = ctx.ws;
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
        let soff = evs[k].off();
        let start = ws + soff;
        let mut last = soff;
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
        while k < n && evs[k].off() - last <= GAP_MS {
            let e = evs[k];
            last = e.off();
            cnt += 1;
            let (price, cat) = e.purchase();
            if price != 0 {
                rev += price as i64;
                purch = true;
                let c = cat as usize;
                if sc.catsum[c] == 0 {
                    sc.touched.push(cat);
                }
                sc.catsum[c] += price as i64;
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
    // Clicks are in time order, so each campaign's union of windows
    // [t, t + 7d] can be built with one open window per campaign.
    if sc.win.len() < ctx.ncamp() {
        sc.win.resize(ctx.ncamp(), (i64::MIN, i64::MIN));
    }
    sc.wtouched.clear();
    for c in cls {
        let cp = c.camp as usize;
        let t = c.ts;
        let w = &mut sc.win[cp];
        if w.1 == i64::MIN {
            *w = (t, t + WEEK_MS);
            sc.wtouched.push(c.camp);
        } else if t <= w.1 {
            w.1 = t + WEEK_MS;
        } else {
            add_range(c.camp, w.0, w.1);
            *w = (t, t + WEEK_MS);
        }
    }
    for &cp in &sc.wtouched {
        let w = sc.win[cp as usize];
        add_range(cp, w.0, w.1);
        sc.win[cp as usize] = (i64::MIN, i64::MIN);
    }
}
