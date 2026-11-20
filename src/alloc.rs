//! Global allocator: big blocks come straight from mmap with MADV_HUGEPAGE
//! (fewer TLB misses on the large hash tables and partition buffers), and
//! grow with mremap instead of copying. Small blocks use the system allocator.

use std::alloc::{GlobalAlloc, Layout, System};

pub struct BigAlloc;

const BIG: usize = 64 << 10;
const PAGE: usize = 4096;

#[inline]
fn round(n: usize) -> usize {
    (n + PAGE - 1) & !(PAGE - 1)
}

unsafe fn map(size: usize) -> *mut u8 {
    unsafe {
        let p = libc::mmap(
            std::ptr::null_mut(),
            size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        );
        if p == libc::MAP_FAILED {
            return std::ptr::null_mut();
        }
        libc::madvise(p, size, libc::MADV_HUGEPAGE);
        p as *mut u8
    }
}

unsafe impl GlobalAlloc for BigAlloc {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        unsafe {
            if l.size() >= BIG && l.align() <= PAGE {
                map(round(l.size()))
            } else {
                System.alloc(l)
            }
        }
    }

    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        unsafe {
            if l.size() >= BIG && l.align() <= PAGE {
                map(round(l.size())) // anonymous mappings are zeroed
            } else {
                System.alloc_zeroed(l)
            }
        }
    }

    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe {
            if l.size() >= BIG && l.align() <= PAGE {
                libc::munmap(p as *mut libc::c_void, round(l.size()));
            } else {
                System.dealloc(p, l)
            }
        }
    }

    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        unsafe {
            let old_big = l.size() >= BIG && l.align() <= PAGE;
            let new_big = new >= BIG && l.align() <= PAGE;
            if old_big && new_big {
                let q = libc::mremap(p as *mut libc::c_void, round(l.size()), round(new), libc::MREMAP_MAYMOVE);
                if q == libc::MAP_FAILED {
                    return std::ptr::null_mut();
                }
                libc::madvise(q, round(new), libc::MADV_HUGEPAGE);
                return q as *mut u8;
            }
            let nl = Layout::from_size_align_unchecked(new, l.align());
            let q = self.alloc(nl);
            if !q.is_null() {
                std::ptr::copy_nonoverlapping(p, q, l.size().min(new));
                self.dealloc(p, l);
            }
            q
        }
    }
}
