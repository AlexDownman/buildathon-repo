//! Radix partitioner with per-bucket write-combining stages: records are
//! staged K at a time in a small cache-resident buffer, then copied with
//! non-temporal stores into the bucket's block list.
//!
//! Bucket storage is a list of fixed 16 KB blocks carved from 2 MB-aligned,
//! huge-page-backed arenas. Blocks are returned to a global pool once a later
//! phase has consumed them, so the next phase reuses memory that is already
//! faulted in instead of zero-filling fresh pages.

use std::sync::Mutex;

pub const BLK: usize = 16 << 10;
const ARENA: usize = 32 << 20;
const HUGE: usize = 2 << 20;

static FREE: Mutex<Vec<usize>> = Mutex::new(Vec::new());

/// Per-thread cache of free blocks, so workers rarely touch the global lock.
/// A thread hands its leftovers back to the global pool when it exits.
struct LocalCache(Vec<usize>);

impl Drop for LocalCache {
    fn drop(&mut self) {
        if !self.0.is_empty() {
            FREE.lock().unwrap().extend_from_slice(&self.0);
        }
    }
}

thread_local! {
    static CACHE: std::cell::RefCell<LocalCache> = const { std::cell::RefCell::new(LocalCache(Vec::new())) };
}

fn new_arena(into: &mut Vec<usize>) {
    unsafe {
        let raw = libc::mmap(
            std::ptr::null_mut(),
            ARENA + HUGE,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        );
        assert!(raw != libc::MAP_FAILED, "mmap arena failed");
        let a = (raw as usize + HUGE - 1) & !(HUGE - 1);
        if crate::alloc::HUGE.load(std::sync::atomic::Ordering::Relaxed) {
            libc::madvise(a as *mut libc::c_void, ARENA, libc::MADV_HUGEPAGE);
        }
        // handed out from the low end first, so pages are touched in order
        for i in (0..ARENA / BLK).rev() {
            into.push(a + i * BLK);
        }
    }
}

#[inline]
fn get_block() -> *mut u8 {
    CACHE.with(|c| {
        let v = &mut c.borrow_mut().0;
        if let Some(p) = v.pop() {
            return p as *mut u8;
        }
        {
            let mut f = FREE.lock().unwrap();
            let k = f.len().saturating_sub(256);
            v.extend(f.drain(k..));
        }
        if v.is_empty() {
            new_arena(v);
        }
        v.pop().unwrap() as *mut u8
    })
}

fn put_blocks(ps: &[usize]) {
    FREE.lock().unwrap().extend_from_slice(ps);
}

/// Append-only vector stored in pool blocks.
pub struct BlockVec<T> {
    blocks: Vec<usize>,
    len: usize,
    per: usize,
    _t: std::marker::PhantomData<T>,
}

unsafe impl<T: Send> Send for BlockVec<T> {}
unsafe impl<T: Sync> Sync for BlockVec<T> {}

impl<T: Copy> BlockVec<T> {
    /// `per` records per block, a multiple of `k` (the stage size)
    pub fn new(k: usize) -> Self {
        let per = (BLK / (std::mem::size_of::<T>() * k)) * k;
        BlockVec { blocks: Vec::new(), len: 0, per, _t: std::marker::PhantomData }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    fn slot(&mut self) -> *mut T {
        let off = self.len % self.per;
        if off == 0 {
            self.blocks.push(get_block() as usize);
        }
        unsafe { (*self.blocks.last().unwrap() as *mut T).add(off) }
    }

    /// Appends K records; len must be a multiple of K (always true before finish).
    #[inline(always)]
    pub fn push_k<const K: usize>(&mut self, st: &[T; K]) {
        let dst = self.slot() as *mut u8;
        let bytes = std::mem::size_of::<T>() * K;
        unsafe {
            #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
            {
                if bytes % 32 == 0 && (dst as usize) % 32 == 0 {
                    use std::arch::x86_64::*;
                    let src = st.as_ptr() as *const u8;
                    let mut o = 0;
                    while o < bytes {
                        let x = _mm256_loadu_si256(src.add(o) as *const __m256i);
                        _mm256_stream_si256(dst.add(o) as *mut __m256i, x);
                        o += 32;
                    }
                    self.len += K;
                    return;
                }
            }
            std::ptr::copy_nonoverlapping(st.as_ptr() as *const u8, dst, bytes);
        }
        self.len += K;
    }

    pub fn push(&mut self, v: T) {
        let p = self.slot();
        unsafe { p.write(v) };
        self.len += 1;
    }

    /// Raw pointer to record i (for in-place marking from other threads).
    #[inline(always)]
    pub fn ptr(&self, i: usize) -> *mut T {
        (self.blocks[i / self.per] as *mut T).wrapping_add(i % self.per)
    }

    #[inline(always)]
    pub fn get(&self, i: usize) -> &T {
        unsafe { &*((self.blocks[i / self.per] as *const T).add(i % self.per)) }
    }

    /// The stored records, block by block.
    pub fn slices(&self) -> impl Iterator<Item = &[T]> + '_ {
        let per = self.per;
        let len = self.len;
        self.blocks.iter().enumerate().map(move |(i, &p)| {
            let n = (len - i * per).min(per);
            unsafe { std::slice::from_raw_parts(p as *const T, n) }
        })
    }

    /// Returns the blocks to the pool.
    pub fn release(self) {
        put_blocks(&self.blocks);
    }
}

pub struct Parts<T: Copy + Default, const K: usize> {
    pub bufs: Vec<BlockVec<T>>,
    stage: Vec<[T; K]>,
    cnt: Vec<u32>,
}

impl<T: Copy + Default, const K: usize> Parts<T, K> {
    pub fn new(nb: usize) -> Self {
        Parts { bufs: (0..nb).map(|_| BlockVec::new(K)).collect(), stage: vec![[T::default(); K]; nb], cnt: vec![0; nb] }
    }

    #[inline(always)]
    pub fn push(&mut self, b: usize, v: T) {
        let c = unsafe { self.cnt.get_unchecked_mut(b) };
        let st = unsafe { self.stage.get_unchecked_mut(b) };
        st[*c as usize] = v;
        *c += 1;
        if *c as usize == K {
            *c = 0;
            Self::flush_full(unsafe { self.bufs.get_unchecked_mut(b) }, st);
        }
    }

    /// Like push, returning the record's final index in bucket b.
    #[inline(always)]
    pub fn push_idx(&mut self, b: usize, v: T) -> usize {
        let i = unsafe { self.bufs.get_unchecked(b).len() + *self.cnt.get_unchecked(b) as usize };
        self.push(b, v);
        i
    }

    #[inline(never)]
    fn flush_full(v: &mut BlockVec<T>, st: &[T; K]) {
        v.push_k(st);
    }

    pub fn finish(mut self) -> Vec<BlockVec<T>> {
        for b in 0..self.bufs.len() {
            let c = self.cnt[b] as usize;
            for i in 0..c {
                let x = self.stage[b][i];
                self.bufs[b].push(x);
            }
        }
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_sfence();
        }
        self.bufs
    }
}
