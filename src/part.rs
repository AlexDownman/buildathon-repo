//! Radix partitioner with per-bucket write-combining stages: records are
//! staged K at a time in a small cache-resident buffer, then copied to the
//! bucket's vector with non-temporal stores (no read-for-ownership misses).

pub struct Parts<T: Copy + Default, const K: usize> {
    pub bufs: Vec<Vec<T>>,
    stage: Vec<[T; K]>,
    cnt: Vec<u32>,
}

impl<T: Copy + Default, const K: usize> Parts<T, K> {
    pub fn new(nb: usize, reserve: usize) -> Self {
        Parts {
            bufs: (0..nb).map(|_| Vec::with_capacity(reserve.max(K))).collect(),
            stage: vec![[T::default(); K]; nb],
            cnt: vec![0; nb],
        }
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

    #[inline(never)]
    fn flush_full(v: &mut Vec<T>, st: &[T; K]) {
        v.reserve(K);
        let bytes = std::mem::size_of::<T>() * K;
        unsafe {
            let dst = v.as_mut_ptr().add(v.len()) as *mut u8;
            #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
            {
                if bytes % 64 == 0 && (dst as usize) % 64 == 0 {
                    use std::arch::x86_64::*;
                    let src = st.as_ptr() as *const u8;
                    let mut o = 0;
                    while o < bytes {
                        let x = _mm256_loadu_si256(src.add(o) as *const __m256i);
                        _mm256_stream_si256(dst.add(o) as *mut __m256i, x);
                        o += 32;
                    }
                    v.set_len(v.len() + K);
                    return;
                }
            }
            std::ptr::copy_nonoverlapping(st.as_ptr() as *const u8, dst, bytes);
            v.set_len(v.len() + K);
        }
    }

    pub fn finish(mut self) -> Vec<Vec<T>> {
        for b in 0..self.bufs.len() {
            let c = self.cnt[b] as usize;
            self.bufs[b].extend_from_slice(&self.stage[b][..c]);
        }
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_sfence();
        }
        self.bufs
    }
}
