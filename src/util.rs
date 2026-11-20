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
        let mask = !0u128 << (8 * (16 - n));
        (v & mask) | n as u128
    } else {
        key_of(b)
    }
}

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

#[inline(always)]
pub fn hash128(k: u128) -> u64 {
    let lo = k as u64;
    let hi = (k >> 64) as u64;
    let m = ((lo ^ 0x243F_6A88_85A3_08D3) as u128).wrapping_mul((hi ^ 0x1319_8A2E_0370_7344) as u128);
    let mut x = (m as u64) ^ ((m >> 64) as u64) ^ lo.rotate_left(17);
    x ^= x >> 32;
    x = x.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^= x >> 29;
    x
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

    #[inline(always)]
    pub fn prefetch(&self, h: u64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            let p = self.slots.as_ptr().add((h as usize) & self.mask) as *const i8;
            std::arch::x86_64::_mm_prefetch(p, std::arch::x86_64::_MM_HINT_T0);
        }
    }

    #[inline(always)]
    pub fn get_h(&self, k: u128, h: u64) -> Option<V> {
        let mut i = (h as usize) & self.mask;
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
        if (self.len + 1) * 2 > self.slots.len() {
            self.grow();
        }
        let mut i = (hash128(k) as usize) & self.mask;
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

    pub fn len(&self) -> usize {
        self.len
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
pub fn for_lines(data: &[u8], s: usize, e: usize, mut f: impl FnMut(&[u8])) {
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
// Fast row iterator: 128-byte SIMD window per line, scalar fallback.
// ---------------------------------------------------------------------------

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
unsafe fn masks128(p: *const u8) -> (u128, u128, u128) {
    use std::arch::x86_64::*;
    unsafe {
        let nl = _mm256_set1_epi8(b'\n' as i8);
        let cm = _mm256_set1_epi8(b',' as i8);
        let qt = _mm256_set1_epi8(b'"' as i8);
        let (mut n, mut c, mut q) = (0u128, 0u128, 0u128);
        for k in 0..4 {
            let v = _mm256_loadu_si256(p.add(32 * k) as *const __m256i);
            n |= ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, nl)) as u32) as u128) << (32 * k);
            c |= ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, cm)) as u32) as u128) << (32 * k);
            q |= ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, qt)) as u32) as u128) << (32 * k);
        }
        (n, c, q)
    }
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
#[inline(always)]
unsafe fn masks128(p: *const u8) -> (u128, u128, u128) {
    let (mut n, mut c, mut q) = (0u128, 0u128, 0u128);
    for i in 0..128 {
        let b = unsafe { *p.add(i) };
        n |= ((b == b'\n') as u128) << i;
        c |= ((b == b',') as u128) << i;
        q |= ((b == b'"') as u128) << i;
    }
    (n, c, q)
}

#[inline(always)]
fn below(x: usize) -> u128 {
    (1u128 << x) - 1
}

/// Calls f with the N fields of every well-formed line whose first byte lies
/// in [s, e). The file's first line (the header) is skipped.
#[inline(always)]
pub fn for_rows<'a, const N: usize>(data: &'a [u8], s: usize, e: usize, mut f: impl FnMut(&[&'a [u8]; N])) {
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
    let mut out: [&'a [u8]; N] = [b""; N];
    while pos < e {
        if pos + 128 <= len {
            let (nl, cm, qt) = unsafe { masks128(data.as_ptr().add(pos)) };
            if nl != 0 {
                let ll = nl.trailing_zeros() as usize;
                let lm = below(ll);
                let mut c = cm & lm;
                let q = qt & lm;
                if c.count_ones() as usize == N - 1 && ll > 0 {
                    let line = &data[pos..pos + ll];
                    let mut a = 0usize;
                    let mut ok = true;
                    for (i, slot) in out.iter_mut().enumerate() {
                        let b = if i + 1 < N {
                            let t = c.trailing_zeros() as usize;
                            c &= c.wrapping_sub(1);
                            t
                        } else {
                            ll
                        };
                        let fq = q & below(b) & !below(a);
                        let isq = (fq != 0) as usize;
                        let want = (1u128 << a) | (1u128 << (b.wrapping_sub(1) & 127));
                        ok &= (fq == 0) | ((b >= a + 2) & (fq == want));
                        let lo = a + isq;
                        let hi = (b - isq).max(lo);
                        *slot = unsafe { line.get_unchecked(lo..hi) };
                        a = b + 1;
                    }
                    if ok {
                        f(&out);
                    }
                }
                pos += ll + 1;
                continue;
            }
        }
        let end = match memchr::memchr(b'\n', &data[pos..]) {
            Some(i) => pos + i,
            None => len,
        };
        if split(&data[pos..end], &mut out) {
            f(&out);
        }
        pos = end + 1;
    }
}
