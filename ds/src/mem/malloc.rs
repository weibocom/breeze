pub struct HeapStats {
    pub total: usize,
    pub used: usize,
    pub total_objects: usize,
    pub used_objects: usize,
}


pub use inner::*;
#[cfg(feature = "heap-stats")]
mod inner {
    use cache_padded::CachePadded;
    use jemallocator::Jemalloc;
    use std::alloc::{GlobalAlloc, Layout};
    use std::sync::atomic::{AtomicU64, Ordering::*};
    static ALLOC: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_OBJ: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_OBJ: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    
    static ALLOC_SIZE_1_7: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_8_15: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_16_31: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_32_63: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_64_127: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_128_255: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_256_511: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_512_1023: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_1K_2K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_2K_4K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_4K_8K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_8K_16K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_16K_32K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_32K_64K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_64K_128K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_128K_256K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_256K_512K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_512K_1024K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_1M_2M: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_2M_4M: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_4M_8M: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_8M_16M: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_16M_32M: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_32M_64M: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_64M_MORE: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_TOTAL: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_1_7: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_8_15: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_16_31: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_32_63: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_64_127: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_128_255: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_256_511: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_512_1023: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_1K_2K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_2K_4K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_4K_8K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_8K_16K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_16K_32K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_32K_64K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_64K_128K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_128K_256K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_256K_512K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_512K_1024K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_1M_2M: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_2M_4M: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_4M_8M: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_8M_16M: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_16M_32M: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_32M_64M: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_64M_MORE: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_TOTAL: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    

    struct MemStats;
    impl MemStats {
        #[cfg(feature = "malloc-stats")]
        #[inline(always)]
        fn alloc(&self, size: usize) {
            ALLOC_TOTAL.fetch_add(size as u64, Relaxed);
            match size {
                1..=7 => ALLOC_SIZE_1_7.fetch_add(size as u64, Relaxed),
                8..=15 => ALLOC_SIZE_8_15.fetch_add(size as u64, Relaxed),
                16..=31 => ALLOC_SIZE_16_31.fetch_add(size as u64, Relaxed),
                32..=63 => ALLOC_SIZE_32_63.fetch_add(size as u64, Relaxed),
                64..=127 => ALLOC_SIZE_64_127.fetch_add(size as u64, Relaxed),
                128..=255 => ALLOC_SIZE_128_255.fetch_add(size as u64, Relaxed),
                256..=511 => ALLOC_SIZE_256_511.fetch_add(size as u64, Relaxed),
                512..=1023 => ALLOC_SIZE_512_1023.fetch_add(size as u64, Relaxed),
                1024..=2047 => ALLOC_SIZE_1K_2K.fetch_add(size as u64, Relaxed),
                2048..=4095 => ALLOC_SIZE_2K_4K.fetch_add(size as u64, Relaxed),
                4096..=8191 => ALLOC_SIZE_4K_8K.fetch_add(size as u64, Relaxed),
                8192..=16383 => ALLOC_SIZE_8K_16K.fetch_add(size as u64, Relaxed),
                16384..=32767 => ALLOC_SIZE_16K_32K.fetch_add(size as u64, Relaxed),
                32768..=65535 => ALLOC_SIZE_32K_64K.fetch_add(size as u64, Relaxed),
                65536..=131071 => ALLOC_SIZE_64K_128K.fetch_add(size as u64, Relaxed),
                131072..=262143 => ALLOC_SIZE_128K_256K.fetch_add(size as u64, Relaxed),
                262144..=524287 => ALLOC_SIZE_256K_512K.fetch_add(size as u64, Relaxed),
                524288..=1048575 => ALLOC_SIZE_512K_1024K.fetch_add(size as u64, Relaxed),
                1048576..=2097151 => ALLOC_SIZE_1M_2M.fetch_add(size as u64, Relaxed),
                2097152..=4194303 => ALLOC_SIZE_2M_4M.fetch_add(size as u64, Relaxed),
                4194304..=8388607 => ALLOC_SIZE_4M_8M.fetch_add(size as u64, Relaxed),
                8388608..=16777215 => ALLOC_SIZE_8M_16M.fetch_add(size as u64, Relaxed),
                16777216..=33554431 => ALLOC_SIZE_16M_32M.fetch_add(size as u64, Relaxed),
                33554432..=67108863 => ALLOC_SIZE_32M_64M.fetch_add(size as u64, Relaxed),
                _ => ALLOC_SIZE_64M_MORE.fetch_add(size as u64, Relaxed),
            };
        }
        #[cfg(feature = "malloc-stats")]
        #[inline(always)]
        fn free(&self, size: usize) {
            FREE_TOTAL.fetch_add(size as u64, Relaxed);
            match size {
                1..=7 => FREE_SIZE_1_7.fetch_add(size as u64, Relaxed),
                8..=15 => FREE_SIZE_8_15.fetch_add(size as u64, Relaxed),
                16..=31 => FREE_SIZE_16_31.fetch_add(size as u64, Relaxed),
                32..=63 => FREE_SIZE_32_63.fetch_add(size as u64, Relaxed),
                64..=127 => FREE_SIZE_64_127.fetch_add(size as u64, Relaxed),
                128..=255 => FREE_SIZE_128_255.fetch_add(size as u64, Relaxed),
                256..=511 => FREE_SIZE_256_511.fetch_add(size as u64, Relaxed),
                512..=1023 => FREE_SIZE_512_1023.fetch_add(size as u64, Relaxed),
                1024..=2047 => FREE_SIZE_1K_2K.fetch_add(size as u64, Relaxed),
                2048..=4095 => FREE_SIZE_2K_4K.fetch_add(size as u64, Relaxed),
                4096..=8191 => FREE_SIZE_4K_8K.fetch_add(size as u64, Relaxed),
                8192..=16383 => FREE_SIZE_8K_16K.fetch_add(size as u64, Relaxed),
                16384..=32767 => FREE_SIZE_16K_32K.fetch_add(size as u64, Relaxed),
                32768..=65535 => FREE_SIZE_32K_64K.fetch_add(size as u64, Relaxed),
                65536..=131071 => FREE_SIZE_64K_128K.fetch_add(size as u64, Relaxed),
                131072..=262143 => FREE_SIZE_128K_256K.fetch_add(size as u64, Relaxed),
                262144..=524287 => FREE_SIZE_256K_512K.fetch_add(size as u64, Relaxed),
                524288..=1048575 => FREE_SIZE_512K_1024K.fetch_add(size as u64, Relaxed),
                1048576..=2097151 => FREE_SIZE_1M_2M.fetch_add(size as u64, Relaxed),
                2097152..=4194303 => FREE_SIZE_2M_4M.fetch_add(size as u64, Relaxed),
                4194304..=8388607 => FREE_SIZE_4M_8M.fetch_add(size as u64, Relaxed),
                8388608..=16777215 => FREE_SIZE_8M_16M.fetch_add(size as u64, Relaxed),
                16777216..=33554431 => FREE_SIZE_16M_32M.fetch_add(size as u64, Relaxed),
                33554432..=67108863 => FREE_SIZE_32M_64M.fetch_add(size as u64, Relaxed),
                _ => FREE_SIZE_64M_MORE.fetch_add(size as u64, Relaxed),
            };
        }

        #[cfg(not(feature = "malloc-stats"))]
        #[inline(always)]
        fn alloc(&self, size: usize) {}

        #[cfg(not(feature = "malloc-stats"))]
        #[inline(always)]
        fn free(&self, size: usize) {}
    }

    struct Stats;
    impl Stats {
        #[inline(always)]
        fn alloc(&self, size: usize) {
            ALLOC.fetch_add(size as u64, Relaxed);
            ALLOC_OBJ.fetch_add(1, Relaxed);
            MemStats.alloc(size);
        }
        #[inline(always)]
        fn free(&self, size: usize) {
            FREE.fetch_add(size as u64, Relaxed);
            FREE_OBJ.fetch_add(1, Relaxed);
            MemStats.free(size);
        }
    }
    pub struct BrzMalloc;
    unsafe impl GlobalAlloc for BrzMalloc {
        #[inline]
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            Stats.alloc(layout.size());
            Jemalloc.alloc(layout)
        }

        #[inline]
        unsafe fn dealloc(&self, ptr: *mut u8, _layout: Layout) {
            Stats.free(_layout.size());
            Jemalloc.dealloc(ptr, _layout)
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

    #[cfg(feature = "malloc-stats")]
    pub fn mem_stats_summary() -> String {
        format!(
r#"{{
    "allocated": {{
        "size [1 ... 7]": "{}"
        "size [8 ... 15]": "{}"
        "size [16 ... 31]": "{}"
        "size [32 ... 63]": "{}"
        "size [64 ... 127]": "{}"
        "size [128 ... 255]": "{}"
        "size [256 ... 511]": "{}"
        "size [512 ... 1023]": "{}"
        "size [1024 ... 2047]": "{}"
        "size [2048 ... 4095]": "{}"
        "size [4096 ... 8191]": "{}"
        "size [8192 ... 16383]": "{}"
        "size [16384 ... 32767]": "{}"
        "size [32768 ... 65535]": "{}"
        "size [65536 ... 131071]": "{}"
        "size [131072 ... 262143]": "{}"
        "size [262144 ... 524287]": "{}"
        "size [524288 ... 1048575]": "{}"
        "size [1048576 ... 2097151]": "{}"
        "size [2097152 ... 4194303]": "{}"
        "size [4194304 ... 8388607]": "{}"
        "size [8388608 ... 16777215]": "{}"
        "size [16777216 ... 33554431]": "{}"
        "size [33554432 ... 67108863]": "{}"
        "size [67108864 ... ]": "{}"
        "total": "{}"
    }},
    "freed": {{
        "size [1 ... 7]": "{}"
        "size [8 ... 15]": "{}"
        "size [16 ... 31]": "{}"
        "size [32 ... 63]": "{}"
        "size [64 ... 127]": "{}"
        "size [128 ... 255]": "{}"
        "size [256 ... 511]": "{}"
        "size [512 ... 1023]": "{}"
        "size [1024 ... 2047]": "{}"
        "size [2048 ... 4095]": "{}"
        "size [4096 ... 8191]": "{}"
        "size [8192 ... 16383]": "{}"
        "size [16384 ... 32767]": "{}"
        "size [32768 ... 65535]": "{}"
        "size [65536 ... 131071]": "{}"
        "size [131072 ... 262143]": "{}"
        "size [262144 ... 524287]": "{}"
        "size [524288 ... 1048575]": "{}"
        "size [1048576 ... 2097151]": "{}"
        "size [2097152 ... 4194303]": "{}"
        "size [4194304 ... 8388607]": "{}"
        "size [8388608 ... 16777215]": "{}"
        "size [16777216 ... 33554431]": "{}"
        "size [33554432 ... 67108863]": "{}"
        "size [67108864 ... ]": "{}"
        "total": "{}"
    }}
}}"#,
        ALLOC_SIZE_1_7.load(Relaxed),
        ALLOC_SIZE_8_15.load(Relaxed),
        ALLOC_SIZE_16_31.load(Relaxed),
        ALLOC_SIZE_32_63.load(Relaxed),
        ALLOC_SIZE_64_127.load(Relaxed),
        ALLOC_SIZE_128_255.load(Relaxed),
        ALLOC_SIZE_256_511.load(Relaxed),
        ALLOC_SIZE_512_1023.load(Relaxed),
        ALLOC_SIZE_1K_2K.load(Relaxed),
        ALLOC_SIZE_2K_4K.load(Relaxed),
        ALLOC_SIZE_4K_8K.load(Relaxed),
        ALLOC_SIZE_8K_16K.load(Relaxed),
        ALLOC_SIZE_16K_32K.load(Relaxed),
        ALLOC_SIZE_32K_64K.load(Relaxed),
        ALLOC_SIZE_64K_128K.load(Relaxed),
        ALLOC_SIZE_128K_256K.load(Relaxed),
        ALLOC_SIZE_256K_512K.load(Relaxed),
        ALLOC_SIZE_512K_1024K.load(Relaxed),
        ALLOC_SIZE_1M_2M.load(Relaxed),
        ALLOC_SIZE_2M_4M.load(Relaxed),
        ALLOC_SIZE_4M_8M.load(Relaxed),
        ALLOC_SIZE_8M_16M.load(Relaxed),
        ALLOC_SIZE_16M_32M.load(Relaxed),
        ALLOC_SIZE_32M_64M.load(Relaxed),
        ALLOC_SIZE_64M_MORE.load(Relaxed),
        ALLOC_TOTAL.load(Relaxed),
        FREE_SIZE_1_7.load(Relaxed),
        FREE_SIZE_8_15.load(Relaxed),
        FREE_SIZE_16_31.load(Relaxed),
        FREE_SIZE_32_63.load(Relaxed),
        FREE_SIZE_64_127.load(Relaxed),
        FREE_SIZE_128_255.load(Relaxed),
        FREE_SIZE_256_511.load(Relaxed),
        FREE_SIZE_512_1023.load(Relaxed),
        FREE_SIZE_1K_2K.load(Relaxed),
        FREE_SIZE_2K_4K.load(Relaxed),
        FREE_SIZE_4K_8K.load(Relaxed),
        FREE_SIZE_8K_16K.load(Relaxed),
        FREE_SIZE_16K_32K.load(Relaxed),
        FREE_SIZE_32K_64K.load(Relaxed),
        FREE_SIZE_64K_128K.load(Relaxed),
        FREE_SIZE_128K_256K.load(Relaxed),
        FREE_SIZE_256K_512K.load(Relaxed),
        FREE_SIZE_512K_1024K.load(Relaxed),
        FREE_SIZE_1M_2M.load(Relaxed),
        FREE_SIZE_2M_4M.load(Relaxed),
        FREE_SIZE_4M_8M.load(Relaxed),
        FREE_SIZE_8M_16M.load(Relaxed),
        FREE_SIZE_16M_32M.load(Relaxed),
        FREE_SIZE_32M_64M.load(Relaxed),
        FREE_SIZE_64M_MORE.load(Relaxed),
        FREE_TOTAL.load(Relaxed),
        )
    }

    #[cfg(not(feature = "malloc-stats"))]
    #[inline(always)]
    pub fn mem_stats_summary() -> String {
        "none".to_string()
    }
    
}
#[cfg(not(feature = "heap-stats"))]
mod inner {
    pub struct HeapStats;
    pub type BrzMalloc = jemallocator::Jemalloc;
    pub fn heap() -> Option<super::HeapStats> {
        None
    }
    pub fn mem_stats_summary() -> String {
        "unknown".to_string()
    }
}
