pub struct HeapStats {
    pub total: usize,
    pub used: usize,
    pub total_objects: usize,
    pub used_objects: usize,
}
pub use inner::*;
#[cfg(feature = "heap-stats")]
mod inner {
    use crossbeam_utils::CachePadded;
    use mimalloc::MiMalloc;
    use std::alloc::{GlobalAlloc, Layout};
    use std::sync::atomic::{AtomicU64, Ordering::*};

    static ALLOC: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_OBJ: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_OBJ: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));

    struct Stats;
    impl Stats {
        #[inline(always)]
        fn alloc(&self, size: usize) {
            ALLOC.fetch_add(size as u64, Relaxed);
            ALLOC_OBJ.fetch_add(1, Relaxed);
        }
        #[inline(always)]
        fn free(&self, size: usize) {
            FREE.fetch_add(size as u64, Relaxed);
            FREE_OBJ.fetch_add(1, Relaxed);
        }
    }
    pub struct BrzMalloc;
    unsafe impl GlobalAlloc for BrzMalloc {
        #[inline]
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            Stats.alloc(layout.size());
            MiMalloc.alloc(layout)
        }

        #[inline]
        unsafe fn dealloc(&self, ptr: *mut u8, _layout: Layout) {
            Stats.free(_layout.size());
            MiMalloc.dealloc(ptr, _layout)
        }
    }

    pub fn heap() -> Option<super::HeapStats> {
        let alloc = ALLOC.load(Relaxed);
        let free = FREE.load(Relaxed);
        let alloc_objects = ALLOC_OBJ.load(Relaxed);
        let free_objects = FREE_OBJ.load(Relaxed);
        Some(super::HeapStats {
            total: alloc as usize,
            used: (alloc - free) as usize,
            total_objects: alloc_objects as usize,
            used_objects: (alloc_objects - free_objects) as usize,
        })
    }
}
#[cfg(not(feature = "heap-stats"))]
mod inner {
    pub struct HeapStats;
    pub type BrzMalloc = mimalloc::MiMalloc;
    pub fn heap() -> Option<super::HeapStats> {
        None
    }
}
