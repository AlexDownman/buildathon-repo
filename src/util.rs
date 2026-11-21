//! Low-level helpers: string keys, hashing, CSV field splitting, timestamps, prices.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{LazyLock, RwLock};

pub const DAY_MS: i64 = 86_400_000;
pub const GAP_MS: i64 = 1_800_000;
pub const WEEK_MS: i64 = 7 * DAY_MS;

// ---------------------------------------------------------------------------
// Keys: every string value becomes an exact u128.
// Strings of <= 15 bytes are packed big-endian with the length in the low byte,
// so u128 order == byte-wise string order. Longer strings (rare) are interned
// in a global table and tagged with 0xFF in the low byte. Key 0 never occurs
// for a non-empty string, so it serves as the "empty slot" sentinel.
// ---------------------------------------------------------------------------

struct LongTab {
    map: HashMap<Vec<u8>, u64>,
    strs: Vec<Vec<u8>>,
}

static LONG: LazyLock<RwLock<LongTab>> =
    LazyLock::new(|| RwLock::new(LongTab { map: HashMap::new(), strs: Vec::new() }));

#[inline(always)]
pub fn key_of(b: &[u8]) -> u128 {
    let n = b.len();
    if n <= 15 {
        let mut a = [0u8; 16];
        a[..n].copy_from_slice(b);
        a[15] = n as u8;
        u128::from_be_bytes(a)
    } else {
        long_key(b)
    }
}

/// Like key_of, but may read 16 bytes from the field start when that stays below `end`.
#[inline(always)]
pub fn key_fast(b: &[u8], end: *const u8) -> u128 {
    let n = b.len();
    if n <= 15 && n > 0 && b.as_ptr().wrapping_add(16) <= end {
        let v = u128::from_be_bytes(unsafe { std::ptr::read_unaligned(b.as_ptr() as *const [u8; 16]) });
        (v & KEYMASK[n]) | n as u128
    } else {
        key_of(b)
    }
}

/// KEYMASK[n] keeps the top n bytes of a big-endian u128
static KEYMASK: [u128; 16] = {
    let mut t = [0u128; 16];
    let mut n = 1;
    while n < 16 {
        t[n] = !0u128 << (8 * (16 - n));
        n += 1;
    }
    t
};

#[cold]
fn long_key(b: &[u8]) -> u128 {
    {
        let t = LONG.read().unwrap();
        if let Some(&id) = t.map.get(b) {
            return ((id as u128) << 8) | 0xFF;
        }
    }
    let mut t = LONG.write().unwrap();
    if let Some(&id) = t.map.get(b) {
        return ((id as u128) << 8) | 0xFF;
    }
    let id = t.strs.len() as u64;
    t.strs.push(b.to_vec());
    t.map.insert(b.to_vec(), id);
    ((id as u128) << 8) | 0xFF
}

#[inline]
pub fn is_long(k: u128) -> bool {
    (k as u8) == 0xFF
}

pub fn key_bytes(k: u128) -> Vec<u8> {
    if is_long(k) {
        let t = LONG.read().unwrap();
        t.strs[(k >> 8) as usize].clone()
    } else {
        let a = k.to_be_bytes();
        a[..a[15] as usize].to_vec()
    }
}

/// Byte-wise comparison of the strings behind two keys.
#[inline]
pub fn cmp_key(a: u128, b: u128) -> Ordering {
    if is_long(a) || is_long(b) {
        if a == b {
            return Ordering::Equal;
        }
        key_bytes(a).cmp(&key_bytes(b))
    } else {
        a.cmp(&b)
    }
}

/// Cheap 128 -> 64 bit hash: good top bits (bucket choice) and, after the
/// final fold, good low bits (table slots).
#[inline(always)]
pub fn hash128(k: u128) -> u64 {
    let lo = k as u64;
    let hi = (k >> 64) as u64;
    let m = lo.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ hi.wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
    m ^ (m >> 29) ^ (m >> 47)
}

// ---------------------------------------------------------------------------
// Open-addressing hash map keyed by u128 (0 = empty).
// ---------------------------------------------------------------------------

pub struct KeyMap<V: Copy> {
    slots: Vec<(u128, V)>,
    mask: usize,
    len: usize,
}

impl<V: Copy + Default> KeyMap<V> {
    pub fn with_capacity(n: usize) -> Self {
        let cap = (n.max(8) * 2).next_power_of_two();
        KeyMap { slots: vec![(0u128, V::default()); cap], mask: cap - 1, len: 0 }
    }

    #[inline(always)]
    pub fn get(&self, k: u128) -> Option<V> {
        let mut i = (hash128(k) as usize) & self.mask;
        loop {
            let s = unsafe { self.slots.get_unchecked(i) };
            if s.0 == k {
                return Some(s.1);
            }
            if s.0 == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }

    /// Returns (value, inserted_now).
    pub fn get_or_insert(&mut self, k: u128, v: V) -> (V, bool) {
        self.get_or_insert_h(k, hash128(k), v)
    }

    pub fn get_or_insert_h(&mut self, k: u128, h: u64, v: V) -> (V, bool) {
        if (self.len + 1) * 2 > self.slots.len() {
            self.grow();
        }
        let mut i = (h as usize) & self.mask;
        loop {
            let s = &mut self.slots[i];
            if s.0 == k {
                return (s.1, false);
            }
            if s.0 == 0 {
                *s = (k, v);
                self.len += 1;
                return (v, true);
            }
            i = (i + 1) & self.mask;
        }
    }

    fn grow(&mut self) {
        let old = std::mem::take(&mut self.slots);
        let cap = old.len() * 2;
        self.slots = vec![(0u128, V::default()); cap];
        self.mask = cap - 1;
        self.len = 0;
        for (k, v) in old {
            if k != 0 {
                self.get_or_insert(k, v);
            }
        }
    }

}

// ---------------------------------------------------------------------------
// CSV: split a line into exactly N fields following rules.md's dialect.
// Returns false when the line is malformed. Unquoted values are returned
// verbatim; a field wrapped in quotes loses its quotes; an empty slice is null.
// ---------------------------------------------------------------------------

#[inline(always)]
fn finish(line: &[u8], a: usize, b: usize, q: u32) -> Option<&[u8]> {
    if q == 0 {
        Some(&line[a..b])
    } else if q == 2 && b - a >= 2 && line[a] == b'"' && line[b - 1] == b'"' {
        Some(&line[a + 1..b - 1])
    } else {
        None
    }
}

#[inline(always)]
pub fn split<'a, const N: usize>(line: &'a [u8], out: &mut [&'a [u8]; N]) -> bool {
    let mut nf = 0usize;
    let mut start = 0usize;
    let mut q = 0u32;
    for (i, &c) in line.iter().enumerate() {
        if c == b',' {
            if nf + 1 >= N {
                return false;
            }
            match finish(line, start, i, q) {
                Some(v) => out[nf] = v,
                None => return false,
            }
            nf += 1;
            start = i + 1;
            q = 0;
        } else if c == b'"' {
            q += 1;
        }
    }
    if nf + 1 != N {
        return false;
    }
    match finish(line, start, line.len(), q) {
        Some(v) => {
            out[nf] = v;
            true
        }
        None => false,
    }
}

/// Calls f on every line whose first byte lies in [s, e). The first line of
/// the file (the header) is skipped.
#[inline]
pub fn for_lines<'a>(data: &'a [u8], s: usize, e: usize, mut f: impl FnMut(&'a [u8])) {
    let mut pos = if s == 0 {
        match memchr::memchr(b'\n', data) {
            Some(i) => i + 1,
            None => return,
        }
    } else if data[s - 1] == b'\n' {
        s
    } else {
        match memchr::memchr(b'\n', &data[s..]) {
            Some(i) => s + i + 1,
            None => return,
        }
    };
    while pos < e {
        let end = match memchr::memchr(b'\n', &data[pos..]) {
            Some(i) => pos + i,
            None => data.len(),
        };
        f(&data[pos..end]);
        pos = end + 1;
    }
}

// ---------------------------------------------------------------------------
// Timestamps
// ---------------------------------------------------------------------------

#[inline]
fn is_leap(y: i64) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

#[inline]
fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if is_leap(y) { 29 } else { 28 }
        }
    }
}

#[inline]
pub const fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

const TY0: i64 = 1900;
const TNY: usize = 400;
static MONTH_START: [i32; TNY * 12 + 1] = build_month_table();

const fn build_month_table() -> [i32; TNY * 12 + 1] {
    let mut t = [0i32; TNY * 12 + 1];
    let mut i = 0;
    while i <= TNY * 12 {
        let y = TY0 + (i / 12) as i64;
        let m = (i % 12) as i64 + 1;
        t[i] = days_from_civil(y, m, 1) as i32;
        i += 1;
    }
    t
}

/// Parses F1/F2/F3 into epoch milliseconds. None = bad timestamp
/// (values too large to matter are also None: they are outside any window).
#[inline]
pub fn parse_ts(f: &[u8]) -> Option<i64> {
    let n = f.len();
    if (n == 20 || n == 25) && f[4] == b'-' {
        let g = |i: usize| f[i].wrapping_sub(b'0');
        let ds = [g(0), g(1), g(2), g(3), g(5), g(6), g(8), g(9), g(11), g(12), g(14), g(15), g(17), g(18)];
        let mut bad = (f[7] != b'-') | (f[10] != b'T') | (f[13] != b':') | (f[16] != b':');
        for &x in &ds {
            bad |= x > 9;
        }
        if bad {
            return None;
        }
        let v = |i: usize| ds[i] as i64;
        let y = v(0) * 1000 + v(1) * 100 + v(2) * 10 + v(3);
        let mo = v(4) * 10 + v(5);
        let d = v(6) * 10 + v(7);
        let h = v(8) * 10 + v(9);
        let mi = v(10) * 10 + v(11);
        let s = v(12) * 10 + v(13);
        if !(1..=12).contains(&mo) || d < 1 || h > 23 || mi > 59 || s > 59 {
            return None;
        }
        let days = if (TY0..TY0 + TNY as i64).contains(&y) {
            let i = ((y - TY0) * 12 + mo - 1) as usize;
            let ms = MONTH_START[i] as i64;
            if d > MONTH_START[i + 1] as i64 - ms {
                return None;
            }
            ms + d - 1
        } else {
            if d > days_in_month(y, mo) {
                return None;
            }
            days_from_civil(y, mo, d)
        };
        let mut t = days * 86_400 + h * 3600 + mi * 60 + s;
        if n == 20 {
            if f[19] != b'Z' {
                return None;
            }
        } else {
            let sign = f[19];
            let a = f[20].wrapping_sub(b'0');
            let b = f[21].wrapping_sub(b'0');
            let c = f[23].wrapping_sub(b'0');
            let e = f[24].wrapping_sub(b'0');
            if (sign != b'+' && sign != b'-') || f[22] != b':' || a > 9 || b > 9 || c > 9 || e > 9 {
                return None;
            }
            let oh = a as i64 * 10 + b as i64;
            let om = c as i64 * 10 + e as i64;
            if oh > 23 || om > 59 {
                return None;
            }
            let off = oh * 3600 + om * 60;
            if sign == b'+' {
                t -= off;
            } else {
                t += off;
            }
        }
        return Some(t * 1000);
    }
    // F3: digits only
    if n == 0 {
        return None;
    }
    let mut v: i64 = 0;
    let mut sig = 0u32;
    for &c in f {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        if sig > 0 || d != 0 {
            sig += 1;
            if sig <= 18 {
                v = v * 10 + d as i64;
            }
        }
    }
    if sig > 18 {
        return None;
    }
    Some(v)
}

/// Purchase price: ASCII digits only, value 1..=10_000_000.
#[inline]
pub fn parse_price(f: &[u8]) -> Option<u32> {
    if f.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in f {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v * 10 + d as u64;
        if v > 10_000_000 {
            return None;
        }
    }
    if v == 0 { None } else { Some(v as u32) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ts_forms() {
        let f1 = parse_ts(b"2026-10-03T23:58:10Z").unwrap();
        assert_eq!(f1, 1791071890000);
        assert_eq!(parse_ts(b"2026-10-03T19:58:10-04:00"), Some(f1));
        assert_eq!(parse_ts(b"1791071890000"), Some(f1));
        assert_eq!(parse_ts(b"0001791071890000"), Some(f1));
        assert_eq!(parse_ts(b"2026-10-03t23:58:10Z"), None);
        assert_eq!(parse_ts(b"2026-10-03T23:58:10z"), None);
        assert_eq!(parse_ts(b"2026-02-29T00:00:00Z"), None);
        assert!(parse_ts(b"2028-02-29T00:00:00Z").is_some());
        assert_eq!(parse_ts(b"2026-10-03T23:58:10+0200"), None);
        assert_eq!(parse_ts(b"2026-10-03T23:58:10+24:00"), None);
        assert_eq!(parse_ts(b"2026-10-03T23:58:10+05:60"), None);
        assert_eq!(parse_ts(b"2026-10-03T23:58:10-00:00"), Some(f1));
        assert_eq!(parse_ts(b"1791071890000.0"), None);
        assert_eq!(parse_ts(b"-5"), None);
        assert_eq!(parse_ts(b"000000000000000000000001"), Some(1));
        assert_eq!(parse_ts(b""), None);
    }

    #[test]
    fn rows_match_split() {
        let mut x = 0x2545F4914F6CDD1Du64;
        let alpha = b"ab,,\"\" \r";
        for round in 0..1500 {
            let mut data = b"h1,h2,h3\n".to_vec();
            let mut lines = Vec::new();
            let nlines = if round % 5 == 0 { 2000 } else { 50 };
            for _ in 0..nlines {
                x ^= x << 13; x ^= x >> 7; x ^= x << 17;
                let n = if round % 7 == 0 { (x % 200) as usize } else { (x % 24) as usize };
                let mut l = Vec::new();
                let mut y = x;
                for _ in 0..n {
                    y = y.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                    l.push(alpha[((y >> 33) % alpha.len() as u64) as usize]);
                }
                data.extend_from_slice(&l);
                data.push(b'\n');
                lines.push(l);
            }
            let len = data.len();
            data.extend_from_slice(&[0u8; 256]);
            let mut got: Vec<Vec<Vec<u8>>> = Vec::new();
            for_rows::<3>(&data, 0, len, |f| got.push(f.iter().map(|v| v.to_vec()).collect()));
            // chunked: lines starting in [s, e) for a split point
            let mid = 1 + (x as usize) % len.max(2).saturating_sub(1);
            let mut got4: Vec<Vec<Vec<u8>>> = Vec::new();
            for_rows::<3>(&data, 0, mid, |f| got4.push(f.iter().map(|v| v.to_vec()).collect()));
            if mid > 0 {
                for_rows::<3>(&data, mid, len, |f| got4.push(f.iter().map(|v| v.to_vec()).collect()));
            }
            assert_eq!(got4, got);
            let mut want: Vec<Vec<Vec<u8>>> = Vec::new();
            for l in &lines {
                let mut o: [&[u8]; 3] = [b""; 3];
                if split(l, &mut o) {
                    want.push(o.iter().map(|v| v.to_vec()).collect());
                }
            }
            assert_eq!(got, want);
        }
    }

    fn rows_struct_case<const N: usize>(x: &mut u64) {
        let mut data = b"h\n".to_vec();
        let mut lines = Vec::new();
        for _ in 0..3000 {
            let mut l = Vec::new();
            *x ^= *x << 13; *x ^= *x >> 7; *x ^= *x << 17;
            let mut y = *x;
            let mut rnd = |m: u64| {
                y = y.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                (y >> 33) % m
            };
            let nf = if rnd(10) == 0 { N - 1 + rnd(3) as usize } else { N };
            for f in 0..nf {
                if f > 0 {
                    l.push(b',');
                }
                let lim = if rnd(4) == 0 { 40 } else { 14 };
                let flen = rnd(lim) as usize;
                let quoted = rnd(3) == 0;
                if quoted {
                    l.push(b'"');
                }
                for _ in 0..flen {
                    l.push(b"abcdefxyz019-:"[rnd(14) as usize]);
                }
                if quoted && rnd(25) != 0 {
                    l.push(b'"');
                }
                if rnd(60) == 0 {
                    l.push(b"\",\r"[rnd(3) as usize]);
                }
            }
            data.extend_from_slice(&l);
            data.push(b'\n');
            lines.push(l);
        }
        let len = data.len();
        data.extend_from_slice(&[0u8; 256]);
        let mut got: Vec<Vec<Vec<u8>>> = Vec::new();
        for_rows::<N>(&data, 0, len, |f| got.push(f.iter().map(|v| v.to_vec()).collect()));
        let mut want: Vec<Vec<Vec<u8>>> = Vec::new();
        for l in &lines {
            let mut o: [&[u8]; N] = [b""; N];
            if split(l, &mut o) {
                want.push(o.iter().map(|v| v.to_vec()).collect());
            }
        }
        assert_eq!(got, want);
    }

    #[test]
    fn rows_match_split_structured() {
        let mut x = 0x9E3779B97F4A7C15u64;
        for _ in 0..200 {
            rows_struct_case::<7>(&mut x);
            rows_struct_case::<4>(&mut x);
            rows_struct_case::<2>(&mut x);
        }
    }

    #[test]
    fn ts_fast_matches() {
        let cases: Vec<&[u8]> = vec![
            b"2026-10-03T23:58:10Z", b"2026-10-03T19:58:10-04:00", b"1791071890000", b"0001791071890000",
            b"2026-10-03t23:58:10Z", b"2026-10-03T23:58:10z", b"2026-02-29T00:00:00Z", b"2028-02-29T00:00:00Z",
            b"2026-10-03T23:58:10+0200", b"2026-10-03T23:58:10+24:00", b"2026-10-03T23:58:10+05:60",
            b"2026-10-03T23:58:10-00:00", b"1791071890000.0", b"-5", b"000000000000000000000001", b"",
            b"2026-13-03T23:58:10Z", b"2026-12-32T23:58:10Z", b"2026-12-31T24:00:00Z", b"2026-12-31T23:60:00Z",
            b"2026-12-31T23:59:60Z", b"2026-12-31 23:59:59Z", b"2026-1a-31T23:59:59Z", b"2026/12/31T23:59:59Z",
            b"2026-12-31T23:59:59.123Z", b"86400000", b"123456789", b"9999999999999999", b"17910718900\xd9\xa2",
            b"2026-10-03T23:58:10+05:30", b"2026-10-03T23:58:1\xef\xbc\x95Z", b"0000-01-01T00:00:00Z", b"9999-12-31T23:59:59Z",
            b"1791071890000\r", b"2026-10-03T23:58:10Z\r", b"2026-00-03T23:58:10Z", b"2026-10-00T23:58:10Z",
        ];
        for c in cases {
            let mut buf = c.to_vec();
            buf.extend_from_slice(&[b'9'; 32]);
            let f = &buf[..c.len()];
            let end = buf.as_ptr_range().end;
            assert_eq!(parse_ts_fast(f, end), parse_ts(f), "{:?}", String::from_utf8_lossy(c));
        }
        // exhaustive-ish over random digit strings of every length
        let mut x = 88172645463325252u64;
        for _ in 0..200000 {
            x ^= x << 13; x ^= x >> 7; x ^= x << 17;
            let n = (x % 20) as usize;
            let mut buf: Vec<u8> = (0..n).map(|i| b"0123456789x"[((x >> (i * 3)) % 11) as usize]).collect();
            let m = buf.len();
            buf.extend_from_slice(&[b'7'; 32]);
            let end = buf.as_ptr_range().end;
            assert_eq!(parse_ts_fast(&buf[..m], end), parse_ts(&buf[..m]));
        }
        // random single/double corruptions of valid F1/F2 strings
        let bases: [&[u8]; 4] = [b"2028-02-29T23:59:59Z", b"2027-12-31T00:00:00-10:00", b"2025-01-01T12:30:45+12:00", b"2026-02-28T23:59:59Z"];
        let alphabet = b"0123456789-:TZtz+ .,/\x00\xff";
        for _ in 0..300000 {
            x ^= x << 13; x ^= x >> 7; x ^= x << 17;
            let mut v = bases[(x % 4) as usize].to_vec();
            for k in 0..((x >> 2) % 3) {
                let pos = ((x >> (8 + 8 * k)) as usize) % v.len();
                v[pos] = alphabet[((x >> (30 + 5 * k)) as usize) % alphabet.len()];
            }
            let m = v.len();
            v.extend_from_slice(&[b'5'; 32]);
            let end = v.as_ptr_range().end;
            assert_eq!(parse_ts_fast(&v[..m], end), parse_ts(&v[..m]), "{:?}", String::from_utf8_lossy(&v[..m]));
        }
        // random field values (in and out of range) with random trailing bytes
        for _ in 0..400000 {
            x ^= x << 13; x ^= x >> 7; x ^= x << 17;
            let r = |k: u32, m: u64| (x >> k) % m;
            let y = match r(0, 4) { 0 => r(3, 10000), 1 => 1890 + r(5, 30), 2 => 2290 + r(7, 20), _ => 2020 + r(9, 10) };
            let sep = if r(40, 2) == 0 { b'+' } else { b'-' };
            let mut v = format!(
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
                y, r(12, 14), r(16, 33), r(22, 26), r(27, 62), r(33, 62)
            )
            .into_bytes();
            if r(39, 2) == 0 {
                v.push(b'Z');
            } else {
                v.push(sep);
                v.extend_from_slice(format!("{:02}:{:02}", r(41, 26), r(46, 62)).as_bytes());
            }
            let m = v.len();
            let mut z = x;
            for _ in 0..32 {
                z ^= z << 13; z ^= z >> 7; z ^= z << 17;
                v.push(z as u8);
            }
            let end = v.as_ptr_range().end;
            assert_eq!(parse_ts_fast(&v[..m], end), parse_ts(&v[..m]), "{:?}", String::from_utf8_lossy(&v[..m]));
        }
    }

    #[test]
    fn keys_order() {
        let a = key_of(b"a");
        let b = key_of(b"a\0");
        let c = key_of(b"ab");
        let u = key_of(b"U1");
        assert!(a < b && b < c && u < a);
        let l1 = key_of(b"aaaaaaaaaaaaaaaaaaaa");
        let l2 = key_of(b"aaaaaaaaaaaaaaaaaaab");
        assert_eq!(cmp_key(l1, l2), Ordering::Less);
        assert_eq!(cmp_key(a, l1), Ordering::Less);
        assert_eq!(cmp_key(l1, c), Ordering::Less);
        assert_eq!(key_bytes(l2), b"aaaaaaaaaaaaaaaaaaab".to_vec());
    }

    #[test]
    fn split_rules() {
        let mut o: [&[u8]; 3] = [b""; 3];
        assert!(split(b"a,\"b\",", &mut o));
        assert_eq!(o, [&b"a"[..], b"b", b""]);
        assert!(split(b"\"\",x,y", &mut o));
        assert_eq!(o[0], b"");
        assert!(!split(b"a,b", &mut o));
        assert!(!split(b"a,b,c,d", &mut o));
        assert!(!split(b"a,b\",c", &mut o));
        assert!(!split(b"a,\",c", &mut o));
        assert!(!split(b"", &mut o));
        assert!(!split(b" a, \"b\" ,c", &mut o));
        assert!(split(b" a,\" b\",c\r", &mut o));
        assert_eq!(o, [&b" a"[..], b" b", b"c\r"]);
    }
}

// ---------------------------------------------------------------------------
// Fast row iterator: SIMD masks of a 64- or 128-byte window per line, scalar
// fallback for longer lines.
// ---------------------------------------------------------------------------

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
unsafe fn masks64(p: *const u8) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    unsafe {
        let nl = _mm256_set1_epi8(b'\n' as i8);
        let cm = _mm256_set1_epi8(b',' as i8);
        let qt = _mm256_set1_epi8(b'"' as i8);
        let v0 = _mm256_loadu_si256(p as *const __m256i);
        let v1 = _mm256_loadu_si256(p.add(32) as *const __m256i);
        let m = |v: __m256i, c: __m256i| _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, c)) as u32 as u64;
        (m(v0, nl) | m(v1, nl) << 32, m(v0, cm) | m(v1, cm) << 32, m(v0, qt) | m(v1, qt) << 32)
    }
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
#[inline(always)]
unsafe fn masks64(p: *const u8) -> (u64, u64, u64) {
    let (mut n, mut c, mut q) = (0u64, 0u64, 0u64);
    for i in 0..64 {
        let b = unsafe { *p.add(i) };
        n |= ((b == b'\n') as u64) << i;
        c |= ((b == b',') as u64) << i;
        q |= ((b == b'"') as u64) << i;
    }
    (n, c, q)
}

/// Quoting is valid iff every quote sits at its field's first or last byte,
/// the count is even, and no separator lies between an opening and a closing
/// quote (prefix-XOR of the quote mask is 0 at every comma). Together these
/// mean each field has 0 quotes, or exactly 2 at both ends (length >= 2).
/// `lb` is the single bit at the line's newline.
#[inline(always)]
fn quotes_ok(q: u128, c: u128, lb: u128) -> bool {
    let starts = (c << 1) | 1;
    let ends = (c >> 1) | (lb >> 1);
    if q & !(starts | ends) != 0 {
        return false;
    }
    let x = prefix_xor(q);
    // bit 127 of the inclusive prefix-XOR is the parity of the whole mask
    (x & c) == 0 && (x >> 127) == 0
}

/// quotes_ok for a line within 64 bytes.
#[inline(always)]
fn quotes_ok64(q: u64, c: u64, lb: u64) -> bool {
    let starts = (c << 1) | 1;
    let ends = (c >> 1) | (lb >> 1);
    if q & !(starts | ends) != 0 {
        return false;
    }
    let x = prefix_xor64(q);
    (x & c) == 0 && (x >> 63) == 0
}

#[cfg(all(target_arch = "x86_64", target_feature = "pclmulqdq"))]
#[inline(always)]
fn prefix_xor64(q: u64) -> u64 {
    use std::arch::x86_64::*;
    unsafe { _mm_cvtsi128_si64(_mm_clmulepi64_si128(_mm_cvtsi64_si128(q as i64), _mm_set1_epi8(-1), 0x00)) as u64 }
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "pclmulqdq")))]
#[inline(always)]
fn prefix_xor64(q: u64) -> u64 {
    let mut x = q;
    x ^= x << 1;
    x ^= x << 2;
    x ^= x << 4;
    x ^= x << 8;
    x ^= x << 16;
    x ^= x << 32;
    x
}

#[cfg(all(target_arch = "x86_64", target_feature = "pclmulqdq"))]
#[inline(always)]
fn prefix_xor(q: u128) -> u128 {
    use std::arch::x86_64::*;
    unsafe {
        let v = _mm_set_epi64x((q >> 64) as i64, q as i64);
        let ones = _mm_set1_epi8(-1);
        let lo = _mm_cvtsi128_si64(_mm_clmulepi64_si128(v, ones, 0x00)) as u64;
        let hi = _mm_cvtsi128_si64(_mm_clmulepi64_si128(v, ones, 0x01)) as u64;
        let hi = hi ^ 0u64.wrapping_sub(lo >> 63);
        (hi as u128) << 64 | lo as u128
    }
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "pclmulqdq")))]
#[inline(always)]
fn prefix_xor(q: u128) -> u128 {
    let mut x = q;
    x ^= x << 1;
    x ^= x << 2;
    x ^= x << 4;
    x ^= x << 8;
    x ^= x << 16;
    x ^= x << 32;
    x ^= x << 64;
    x
}

/// Position of the lowest set bit of hi:lo (which must be non-zero), cleared
/// afterwards; branch-free so the 64-bit boundary costs no mispredicts.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn next_bit(lo: &mut u64, hi: &mut u64) -> usize {
    let t: u64;
    unsafe {
        std::arch::asm!(
            "tzcnt {t}, {lo}",
            "blsr {hn}, {hi}",
            "tzcnt {th}, {hi}",
            "add {th}, 64",
            "test {lo}, {lo}",
            "cmovz {t}, {th}",
            "cmovz {hi}, {hn}",
            "blsr {lo}, {lo}",
            t = out(reg) t,
            th = out(reg) _,
            hn = out(reg) _,
            lo = inout(reg) *lo,
            hi = inout(reg) *hi,
            options(pure, nomem, nostack),
        );
    }
    t as usize
}

#[cfg(not(target_arch = "x86_64"))]
#[inline(always)]
fn next_bit(lo: &mut u64, hi: &mut u64) -> usize {
    if *lo != 0 {
        let t = lo.trailing_zeros() as usize;
        *lo &= *lo - 1;
        t
    } else {
        let t = 64 + hi.trailing_zeros() as usize;
        *hi &= hi.wrapping_sub(1);
        t
    }
}

/// One well-formed line of N fields, read in order: next() for fields
/// 0..N-1, then last() for field N-1 (a line need not be read to the end).
pub trait Fields<'a> {
    fn next(&mut self) -> &'a [u8];
    fn last(&mut self) -> &'a [u8];
}

/// Receives the fields of each well-formed line (see rows).
pub trait RowSink<'a> {
    fn row<R: Fields<'a>>(&mut self, r: &mut R);
}

/// a field [a, b) of the line at base, quotes removed (quoting already validated)
#[inline(always)]
fn strip<'a>(base: *const u8, a: usize, b: usize) -> &'a [u8] {
    let isq = (unsafe { *base.add(a) } == b'"') as usize;
    unsafe { std::slice::from_raw_parts(base.add(a + isq), b - a - 2 * isq) }
}

pub struct Row64<'a> {
    base: *const u8,
    c: u64,
    a: usize,
    ll: usize,
    _m: std::marker::PhantomData<&'a [u8]>,
}

impl<'a> Fields<'a> for Row64<'a> {
    #[inline(always)]
    fn next(&mut self) -> &'a [u8] {
        let b = self.c.trailing_zeros() as usize;
        self.c &= self.c.wrapping_sub(1);
        let f = strip(self.base, self.a, b);
        self.a = b + 1;
        f
    }
    #[inline(always)]
    fn last(&mut self) -> &'a [u8] {
        strip(self.base, self.a, self.ll)
    }
}

pub struct Row128<'a> {
    base: *const u8,
    lo: u64,
    hi: u64,
    a: usize,
    ll: usize,
    _m: std::marker::PhantomData<&'a [u8]>,
}

impl<'a> Fields<'a> for Row128<'a> {
    #[inline(always)]
    fn next(&mut self) -> &'a [u8] {
        let b = next_bit(&mut self.lo, &mut self.hi);
        let f = strip(self.base, self.a, b);
        self.a = b + 1;
        f
    }
    #[inline(always)]
    fn last(&mut self) -> &'a [u8] {
        strip(self.base, self.a, self.ll)
    }
}

pub struct RowArr<'a, 'b, const N: usize> {
    f: &'b [&'a [u8]; N],
    i: usize,
}

impl<'a, 'b, const N: usize> Fields<'a> for RowArr<'a, 'b, N> {
    #[inline(always)]
    fn next(&mut self) -> &'a [u8] {
        let v = self.f[self.i];
        self.i += 1;
        v
    }
    #[inline(always)]
    fn last(&mut self) -> &'a [u8] {
        self.f[N - 1]
    }
}

/// Calls sink.row for every well-formed N-field line whose first byte lies in
/// [s, e). The file's first line (the header) is skipped. Fields are produced
/// lazily from SIMD masks of a 64- or 128-byte window; longer lines go
/// through split.
#[inline(always)]
pub fn rows<'a, const N: usize, S: RowSink<'a>>(data: &'a [u8], s: usize, e: usize, sink: &mut S) {
    let len = data.len();
    let mut pos = if s == 0 {
        match memchr::memchr(b'\n', data) {
            Some(i) => i + 1,
            None => return,
        }
    } else if data[s - 1] == b'\n' {
        s
    } else {
        match memchr::memchr(b'\n', &data[s..]) {
            Some(i) => s + i + 1,
            None => return,
        }
    };
    while pos < e {
        if pos + 128 <= len {
            let base = unsafe { data.as_ptr().add(pos) };
            let (n0, c0, q0) = unsafe { masks64(base) };
            if n0 != 0 {
                // the line fits in the first 64 bytes
                let ll = n0.trailing_zeros() as usize;
                let lb = n0 & n0.wrapping_neg();
                let lm = lb.wrapping_sub(1);
                let c = c0 & lm;
                let q = q0 & lm;
                if c.count_ones() as usize == N - 1 && ll > 0 && (q == 0 || quotes_ok64(q, c, lb)) {
                    sink.row(&mut Row64 { base, c, a: 0, ll, _m: std::marker::PhantomData });
                }
                pos += ll + 1;
                continue;
            }
            let (n1, c1, q1) = unsafe { masks64(base.add(64)) };
            if n1 != 0 {
                let ll = 64 + n1.trailing_zeros() as usize;
                let nl = (n1 as u128) << 64;
                let lb = nl & nl.wrapping_neg();
                let lm = lb.wrapping_sub(1);
                let c = (c0 as u128 | (c1 as u128) << 64) & lm;
                let q = (q0 as u128 | (q1 as u128) << 64) & lm;
                if c.count_ones() as usize == N - 1 && (q == 0 || quotes_ok(q, c, lb)) {
                    sink.row(&mut Row128 { base, lo: c as u64, hi: (c >> 64) as u64, a: 0, ll, _m: std::marker::PhantomData });
                }
                pos += ll + 1;
                continue;
            }
        }
        let end = match memchr::memchr(b'\n', &data[pos..]) {
            Some(i) => pos + i,
            None => len,
        };
        let mut out: [&'a [u8]; N] = [b""; N];
        if split(&data[pos..end], &mut out) {
            sink.row(&mut RowArr { f: &out, i: 0 });
        }
        pos = end + 1;
    }
}

struct ArrSink<F, const N: usize>(F);

impl<'a, F: FnMut(&[&'a [u8]; N]), const N: usize> RowSink<'a> for ArrSink<F, N> {
    #[inline(always)]
    fn row<R: Fields<'a>>(&mut self, r: &mut R) {
        let mut out: [&'a [u8]; N] = [b""; N];
        for (i, o) in out.iter_mut().enumerate() {
            *o = if i + 1 < N { r.next() } else { r.last() };
        }
        (self.0)(&out);
    }
}

/// Calls f with the N fields of every well-formed line whose first byte lies
/// in [s, e). The file's first line (the header) is skipped.
#[inline(always)]
pub fn for_rows<'a, const N: usize>(data: &'a [u8], s: usize, e: usize, f: impl FnMut(&[&'a [u8]; N])) {
    rows::<N, _>(data, s, e, &mut ArrSink::<_, N>(f));
}

// ---------------------------------------------------------------------------
// SWAR timestamp parsing (same results as parse_ts)
// ---------------------------------------------------------------------------

const ONES: u64 = 0x0101_0101_0101_0101;

#[inline(always)]
fn ld8(p: *const u8) -> u64 {
    u64::from_le_bytes(unsafe { std::ptr::read_unaligned(p as *const [u8; 8]) })
}

/// Bytes selected by `m` (0xFF per byte) are ASCII digits.
#[inline(always)]
fn digits_ok(w: u64, m: u64) -> bool {
    let hi = w & 0xF0F0_F0F0_F0F0_F0F0;
    let hi6 = (w.wrapping_add(0x0606_0606_0606_0606)) & 0xF0F0_F0F0_F0F0_F0F0;
    ((hi ^ (0x30 * ONES)) | (hi6 ^ (0x30 * ONES))) & m & 0xF0F0_F0F0_F0F0_F0F0 == 0
}

/// 8 ASCII digits, first char in the lowest byte -> value.
#[inline(always)]
fn parse8(w: u64) -> u64 {
    let mut v = w.wrapping_sub(0x30 * ONES);
    v = (v.wrapping_mul(10)).wrapping_add(v >> 8) & 0x00FF_00FF_00FF_00FF;
    v = (v.wrapping_mul(100)).wrapping_add(v >> 16) & 0x0000_FFFF_0000_FFFF;
    (v.wrapping_mul(10000)).wrapping_add(v >> 32) & 0xFFFF_FFFF
}

/// ISO forms "YYYY-MM-DDTHH:MM:SSZ" (20) and "...SS+HH:MM" (25) with one
/// 32-byte load; same results as parse_ts. Needs 32 readable bytes at f.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
unsafe fn iso_ts_avx2(f: &[u8]) -> Option<i64> {
    use std::arch::x86_64::*;
    const fn bits(v: &[u32]) -> u32 {
        let mut m = 0;
        let mut i = 0;
        while i < v.len() {
            m |= 1 << v[i];
            i += 1;
        }
        m
    }
    const D20: u32 = bits(&[0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18]);
    const D25X: u32 = bits(&[20, 21, 23, 24]);
    const S_ALL: u32 = bits(&[4, 7, 10, 13, 16]);
    const S20X: u32 = bits(&[19]);
    const S25X: u32 = bits(&[22]);
    // range-check bits (2 per u16 lane) of the offset lanes 10 and 11
    const OFFB: u32 = 0xF << 20;
    let n = f.len();
    let p = f.as_ptr();
    unsafe {
        let v = _mm256_loadu_si256(p as *const __m256i);
        let d = _mm256_sub_epi8(v, _mm256_set1_epi8(b'0' as i8));
        let isdig = _mm256_cmpeq_epi8(_mm256_min_epu8(d, _mm256_set1_epi8(9)), d);
        let tmpl = _mm256_setr_epi8(
            0, 0, 0, 0, b'-' as i8, 0, 0, b'-' as i8, 0, 0, b'T' as i8, 0, 0, b':' as i8, 0, 0, //
            b':' as i8, 0, 0, b'Z' as i8, 0, 0, b':' as i8, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        );
        let issep = _mm256_cmpeq_epi8(v, tmpl);
        let dm = _mm256_movemask_epi8(isdig) as u32;
        let sm = _mm256_movemask_epi8(issep) as u32;
        let l25 = n == 25;
        let dmask = D20 | if l25 { D25X } else { 0 };
        let smask = S_ALL | if l25 { S25X } else { S20X };
        // digits into aligned pairs: lane 0 Y Y Y Y M M D D h h m m, lane 1 s s _ _ oh oh om om
        let idx = _mm256_setr_epi8(
            0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, -1, -1, -1, -1, //
            1, 2, -1, -1, 4, 5, 7, 8, -1, -1, -1, -1, -1, -1, -1, -1,
        );
        let sh = _mm256_shuffle_epi8(d, idx);
        // u16: [YY, YY, M, D, h, m, 0, 0 | s, 0, oh, om, 0, 0, 0, 0]
        let m = _mm256_maddubs_epi16(sh, _mm256_set1_epi16(0x010A));
        let hi = _mm256_setr_epi16(99, 99, 12, 31, 23, 59, 0, 0, 59, 0, 23, 59, 0, 0, 0, 0);
        let lo = _mm256_setr_epi16(0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        let bad = _mm256_or_si256(_mm256_cmpgt_epi16(m, hi), _mm256_cmpgt_epi16(lo, m));
        let bm = _mm256_movemask_epi8(bad) as u32 & if l25 { !0 } else { !OFFB };
        if (dm & dmask) != dmask || (sm & smask) != smask || bm != 0 {
            return None;
        }
        // i32: [year, M, h*3600+m*60, 0 | s, oh*3600+om*60, 0, 0]
        let w = _mm256_setr_epi16(100, 1, 1, 0, 3600, 60, 0, 0, 1, 0, 3600, 60, 0, 0, 0, 0);
        let r = _mm256_madd_epi16(m, w);
        let rl = _mm256_castsi256_si128(r);
        let rh = _mm256_extracti128_si256(r, 1);
        let y = _mm_cvtsi128_si32(rl) as i64;
        let mo = _mm_extract_epi32(rl, 1) as i64;
        let hm = _mm_extract_epi32(rl, 2) as i64;
        let sec = _mm_cvtsi128_si32(rh) as i64;
        let off = _mm_extract_epi32(rh, 1) as i64;
        let dd = _mm256_extract_epi16(m, 3) as i64;
        if !(TY0..TY0 + TNY as i64).contains(&y) {
            return parse_ts(f);
        }
        let i = ((y - TY0) * 12 + mo - 1) as usize;
        let ms = *MONTH_START.get_unchecked(i) as i64;
        if dd > *MONTH_START.get_unchecked(i + 1) as i64 - ms {
            return None;
        }
        let mut t = (ms + dd - 1) * 86_400 + hm + sec;
        if l25 {
            let sg = *p.add(19);
            if sg == b'+' {
                t -= off;
            } else if sg == b'-' {
                t += off;
            } else {
                return None;
            }
        }
        Some(t * 1000)
    }
}

/// Like parse_ts, but may read up to 32 bytes from the field start when that
/// stays below `end`.
#[inline(always)]
pub fn parse_ts_fast(f: &[u8], end: *const u8) -> Option<i64> {
    let n = f.len();
    let p = f.as_ptr();
    if n == 20 || n == 25 {
        #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
        if p.wrapping_add(32) <= end {
            return unsafe { iso_ts_avx2(f) };
        }
        // "YYYY-MM-" "DDTHH:MM" ":SS" + Z | +HH:MM
        let w0 = ld8(p);
        let w1 = ld8(unsafe { p.add(8) });
        let w2 = ld8(unsafe { p.add(11) }); // "HH:MM:SS"
        const SEP0: u64 = (b'-' as u64) << 32 | (b'-' as u64) << 56;
        const M0D: u64 = 0x00FF_FF00_FFFF_FFFF;
        const M0S: u64 = 0xFF00_00FF_0000_0000;
        const SEP1: u64 = (b'T' as u64) << 16 | (b':' as u64) << 40;
        const M1D: u64 = 0xFFFF_00FF_FF00_FFFF;
        const M1S: u64 = 0x0000_FF00_00FF_0000;
        const SEP2: u64 = (b':' as u64) << 16 | (b':' as u64) << 40;
        const M2D: u64 = 0xFFFF_00FF_FF00_FFFF;
        const M2S: u64 = 0x0000_FF00_00FF_0000;
        let ok = digits_ok(w0, M0D)
            & digits_ok(w1, M1D)
            & digits_ok(w2, M2D)
            & ((w0 & M0S) == SEP0)
            & ((w1 & M1S) == SEP1)
            & ((w2 & M2S) == SEP2);
        if !ok {
            return None;
        }
        // digit values with separators zeroed, then byte k := 10*d[k] + d[k+1]
        // (no carries: every byte is <= 9 before, <= 99 after)
        let p0 = (w0 ^ (0x30 * ONES)) & M0D;
        let p1 = (w1 ^ (0x30 * ONES)) & M1D;
        let p2 = (w2 ^ (0x30 * ONES)) & M2D;
        let t0 = p0.wrapping_mul(10).wrapping_add(p0 >> 8);
        let t1 = p1.wrapping_mul(10).wrapping_add(p1 >> 8);
        let t2 = p2.wrapping_mul(10).wrapping_add(p2 >> 8);
        let b = |w: u64, i: u32| ((w >> (8 * i)) & 0xFF) as i64;
        let y = b(t0, 0) * 100 + b(t0, 2);
        let mo = b(t0, 5);
        let d = b(t1, 0);
        let h = b(t1, 3);
        let mi = b(t1, 6);
        let s = b(t2, 6);
        let ok = ((mo as u64).wrapping_sub(1) < 12) & ((d as u64).wrapping_sub(1) < 31) & (h < 24) & (mi < 60) & (s < 60);
        if !ok {
            return None;
        }
        let days = if (TY0..TY0 + TNY as i64).contains(&y) {
            let i = ((y - TY0) * 12 + mo - 1) as usize;
            let ms = MONTH_START[i] as i64;
            if d > MONTH_START[i + 1] as i64 - ms {
                return None;
            }
            ms + d - 1
        } else {
            return parse_ts(f);
        };
        let mut t = days * 86_400 + h * 3600 + mi * 60 + s;
        if n == 20 {
            if f[19] != b'Z' {
                return None;
            }
        } else {
            let sign = f[19];
            let a = f[20].wrapping_sub(b'0');
            let bb = f[21].wrapping_sub(b'0');
            let c = f[23].wrapping_sub(b'0');
            let e = f[24].wrapping_sub(b'0');
            if (sign != b'+' && sign != b'-') || f[22] != b':' || a > 9 || bb > 9 || c > 9 || e > 9 {
                return None;
            }
            let oh = a as i64 * 10 + bb as i64;
            let om = c as i64 * 10 + e as i64;
            if oh > 23 || om > 59 {
                return None;
            }
            let off = oh * 3600 + om * 60;
            if sign == b'+' {
                t -= off;
            } else {
                t += off;
            }
        }
        return Some(t * 1000);
    }
    if (9..=16).contains(&n) && p.wrapping_add(16) <= end {
        // F3: right-align n digits into 16 bytes, left-padded with '0'
        let x = u128::from_le_bytes(unsafe { std::ptr::read_unaligned(p as *const [u8; 16]) });
        let sh = 8 * (16 - n) as u32;
        let x = (x << sh) | (u128::from_le_bytes([b'0'; 16]) & ((1u128 << sh) - 1));
        let lo = x as u64;
        let hi = (x >> 64) as u64;
        if !(digits_ok(lo, !0) & digits_ok(hi, !0)) {
            return None;
        }
        return Some((parse8(lo) * 100_000_000 + parse8(hi)) as i64);
    }
    parse_ts(f)
}
