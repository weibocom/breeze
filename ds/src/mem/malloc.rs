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
    static ALLOC_SIZE_1_8: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_9_16: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_17_32: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_33_64: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_65_128: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_128_256: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_256_512: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_512_1K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_1K_2K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_2K_4K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_4K_8K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_8K_16K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_16K_32K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_32K_64K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_64K_128K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_128K_256K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_256K_512K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_512K_1M: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_1M_2M: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_2M_4M: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_4M_8M: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_8M_16M: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_16M_32M: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_32M_64M: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_SIZE_64M_MORE: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static ALLOC_TOTAL: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_1_8: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_9_16: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_17_32: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_33_64: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_65_128: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_128_256: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_256_512: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_512_1K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_1K_2K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_2K_4K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_4K_8K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_8K_16K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_16K_32K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_32K_64K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_64K_128K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_128K_256K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_256K_512K: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_512K_1M: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_1M_2M: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_2M_4M: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_4M_8M: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_8M_16M: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_16M_32M: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_32M_64M: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_SIZE_64M_MORE: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    static FREE_TOTAL: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
    

    struct Stats;
    impl Stats {
        #[inline(always)]
        fn alloc(&self, size: usize) {
            ALLOC.fetch_add(size as u64, Relaxed);
            ALLOC_OBJ.fetch_add(1, Relaxed);

            ALLOC_TOTAL.fetch_add(size as u64, Relaxed);
            match size {
                1..=8 => ALLOC_SIZE_1_8.fetch_add(size as u64, Relaxed),
                9..=16 => ALLOC_SIZE_9_16.fetch_add(size as u64, Relaxed),
                17..=32 => ALLOC_SIZE_17_32.fetch_add(size as u64, Relaxed),
                33..=64 => ALLOC_SIZE_33_64.fetch_add(size as u64, Relaxed),
                65..=128 => ALLOC_SIZE_65_128.fetch_add(size as u64, Relaxed),
                129..=256 => ALLOC_SIZE_128_256.fetch_add(size as u64, Relaxed),
                257..=512 => ALLOC_SIZE_256_512.fetch_add(size as u64, Relaxed),
                513..=1024 => ALLOC_SIZE_512_1K.fetch_add(size as u64, Relaxed),
                1025..=2048 => ALLOC_SIZE_1K_2K.fetch_add(size as u64, Relaxed),
                2049..=4096 => ALLOC_SIZE_2K_4K.fetch_add(size as u64, Relaxed),
                4097..=8192 => ALLOC_SIZE_4K_8K.fetch_add(size as u64, Relaxed),
                8193..=16384 => ALLOC_SIZE_8K_16K.fetch_add(size as u64, Relaxed),
                16385..=32768 => ALLOC_SIZE_16K_32K.fetch_add(size as u64, Relaxed),
                32769..=65536 => ALLOC_SIZE_32K_64K.fetch_add(size as u64, Relaxed),
                65537..=131072 => ALLOC_SIZE_64K_128K.fetch_add(size as u64, Relaxed),
                131073..=262144 => ALLOC_SIZE_128K_256K.fetch_add(size as u64, Relaxed),
                262145..=524288 => ALLOC_SIZE_256K_512K.fetch_add(size as u64, Relaxed),
                524289..=1048576 => ALLOC_SIZE_512K_1M.fetch_add(size as u64, Relaxed),
                1048577..=2097152 => ALLOC_SIZE_1M_2M.fetch_add(size as u64, Relaxed),
                2097153..=4194304 => ALLOC_SIZE_2M_4M.fetch_add(size as u64, Relaxed),
                4194305..=8388608 => ALLOC_SIZE_4M_8M.fetch_add(size as u64, Relaxed),
                8388609..=16777216 => ALLOC_SIZE_8M_16M.fetch_add(size as u64, Relaxed),
                16777217..=33554432 => ALLOC_SIZE_16M_32M.fetch_add(size as u64, Relaxed),
                33554433..=67108864 => ALLOC_SIZE_32M_64M.fetch_add(size as u64, Relaxed),
                _ => ALLOC_SIZE_64M_MORE.fetch_add(size as u64, Relaxed),
            };
        }
        #[inline(always)]
        fn free(&self, size: usize) {
            FREE.fetch_add(size as u64, Relaxed);
            FREE_OBJ.fetch_add(1, Relaxed);

            FREE_TOTAL.fetch_add(size as u64, Relaxed);
            match size {
                1..=8 => FREE_SIZE_1_8.fetch_add(size as u64, Relaxed),
                9..=16 => FREE_SIZE_9_16.fetch_add(size as u64, Relaxed),
                17..=32 => FREE_SIZE_17_32.fetch_add(size as u64, Relaxed),
                33..=64 => FREE_SIZE_33_64.fetch_add(size as u64, Relaxed),
                65..=128 => FREE_SIZE_65_128.fetch_add(size as u64, Relaxed),
                129..=256 => FREE_SIZE_128_256.fetch_add(size as u64, Relaxed),
                257..=512 => FREE_SIZE_256_512.fetch_add(size as u64, Relaxed),
                513..=1024 => FREE_SIZE_512_1K.fetch_add(size as u64, Relaxed),
                1025..=2048 => FREE_SIZE_1K_2K.fetch_add(size as u64, Relaxed),
                2049..=4096 => FREE_SIZE_2K_4K.fetch_add(size as u64, Relaxed),
                4097..=8192 => FREE_SIZE_4K_8K.fetch_add(size as u64, Relaxed),
                8193..=16384 => FREE_SIZE_8K_16K.fetch_add(size as u64, Relaxed),
                16385..=32768 => FREE_SIZE_16K_32K.fetch_add(size as u64, Relaxed),
                32769..=65536 => FREE_SIZE_32K_64K.fetch_add(size as u64, Relaxed),
                65537..=131072 => FREE_SIZE_64K_128K.fetch_add(size as u64, Relaxed),
                131073..=262144 => FREE_SIZE_128K_256K.fetch_add(size as u64, Relaxed),
                262145..=524288 => FREE_SIZE_256K_512K.fetch_add(size as u64, Relaxed),
                524289..=1048576 => FREE_SIZE_512K_1M.fetch_add(size as u64, Relaxed),
                1048577..=2097152 => FREE_SIZE_1M_2M.fetch_add(size as u64, Relaxed),
                2097153..=4194304 => FREE_SIZE_2M_4M.fetch_add(size as u64, Relaxed),
                4194305..=8388608 => FREE_SIZE_4M_8M.fetch_add(size as u64, Relaxed),
                8388609..=16777216 => FREE_SIZE_8M_16M.fetch_add(size as u64, Relaxed),
                16777217..=33554432 => FREE_SIZE_16M_32M.fetch_add(size as u64, Relaxed),
                33554433..=67108864 => FREE_SIZE_32M_64M.fetch_add(size as u64, Relaxed),
                _ => FREE_SIZE_64M_MORE.fetch_add(size as u64, Relaxed),
            };

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
    pub fn memtrack() -> String {
        format!(
r#"{{
    "allocated": {{
        "size [1 ... 8]": "{}",
        "size [9 ... 16]": "{}",
        "size [17 ... 32]": "{}",
        "size [33 ... 64]": "{}",
        "size [65 ... 128]": "{}",
        "size [129 ... 256]": "{}",
        "size [257 ... 512]": "{}",
        "size [513 ... 1024]": "{}",
        "size [1025 ... 2048]": "{}",
        "size [2049 ... 4096]": "{}",
        "size [4097 ... 8192]": "{}",
        "size [8193 ... 16384]": "{}",
        "size [16385 ... 32768]": "{}",
        "size [32769 ... 65536]": "{}",
        "size [65537 ... 131072]": "{}",
        "size [131073 ... 262144]": "{}",
        "size [262145 ... 524288]": "{}",
        "size [524289 ... 1048576]": "{}",
        "size [1048577 ... 2097152]": "{}",
        "size [2097153 ... 4194304]": "{}",
        "size [4194305 ... 8388608]": "{}",
        "size [8388609 ... 16777216]": "{}",
        "size [16777217 ... 33554432]": "{}",
        "size [33554433 ... 67108864]": "{}",
        "size [67108865 ... ]": "{}",
        "total": "{}",
    }},
    "freed": {{
        "size [1 ... 8]": "{}",
        "size [9 ... 16]": "{}",
        "size [17 ... 32]": "{}",
        "size [33 ... 64]": "{}",
        "size [65 ... 128]": "{}",
        "size [129 ... 256]": "{}",
        "size [257 ... 512]": "{}",
        "size [513 ... 1024]": "{}",
        "size [1025 ... 2048]": "{}",
        "size [2049 ... 4096]": "{}",
        "size [4097 ... 8192]": "{}",
        "size [8193 ... 16384]": "{}",
        "size [16385 ... 32768]": "{}",
        "size [32769 ... 65536]": "{}",
        "size [65537 ... 131072]": "{}",
        "size [131073 ... 262144]": "{}",
        "size [262145 ... 524288]": "{}",
        "size [524289 ... 1048576]": "{}",
        "size [1048577 ... 2097152]": "{}",
        "size [2097153 ... 4194304]": "{}",
        "size [4194305 ... 8388608]": "{}",
        "size [8388609 ... 16777216]": "{}",
        "size [16777217 ... 33554432]": "{}",
        "size [33554433 ... 67108864]": "{}",
        "size [67108865 ... ]": "{}",
        "total": "{}",
    }}
}}"#,
            ALLOC_SIZE_1_8.load(Relaxed),
            ALLOC_SIZE_9_16.load(Relaxed),
            ALLOC_SIZE_17_32.load(Relaxed),
            ALLOC_SIZE_33_64.load(Relaxed),
            ALLOC_SIZE_65_128.load(Relaxed),
            ALLOC_SIZE_128_256.load(Relaxed),
            ALLOC_SIZE_256_512.load(Relaxed),
            ALLOC_SIZE_512_1K.load(Relaxed),
            ALLOC_SIZE_1K_2K.load(Relaxed),
            ALLOC_SIZE_2K_4K.load(Relaxed),
            ALLOC_SIZE_4K_8K.load(Relaxed),
            ALLOC_SIZE_8K_16K.load(Relaxed),
            ALLOC_SIZE_16K_32K.load(Relaxed),
            ALLOC_SIZE_32K_64K.load(Relaxed),
            ALLOC_SIZE_64K_128K.load(Relaxed),
            ALLOC_SIZE_128K_256K.load(Relaxed),
            ALLOC_SIZE_256K_512K.load(Relaxed),
            ALLOC_SIZE_512K_1M.load(Relaxed),
            ALLOC_SIZE_1M_2M.load(Relaxed),
            ALLOC_SIZE_2M_4M.load(Relaxed),
            ALLOC_SIZE_4M_8M.load(Relaxed),
            ALLOC_SIZE_8M_16M.load(Relaxed),
            ALLOC_SIZE_16M_32M.load(Relaxed),
            ALLOC_SIZE_32M_64M.load(Relaxed),
            ALLOC_SIZE_64M_MORE.load(Relaxed),
            ALLOC_TOTAL.load(Relaxed),
            FREE_SIZE_1_8.load(Relaxed),
            FREE_SIZE_9_16.load(Relaxed),
            FREE_SIZE_17_32.load(Relaxed),
            FREE_SIZE_33_64.load(Relaxed),
            FREE_SIZE_65_128.load(Relaxed),
            FREE_SIZE_128_256.load(Relaxed),
            FREE_SIZE_256_512.load(Relaxed),
            FREE_SIZE_512_1K.load(Relaxed),
            FREE_SIZE_1K_2K.load(Relaxed),
            FREE_SIZE_2K_4K.load(Relaxed),
            FREE_SIZE_4K_8K.load(Relaxed),
            FREE_SIZE_8K_16K.load(Relaxed),
            FREE_SIZE_16K_32K.load(Relaxed),
            FREE_SIZE_32K_64K.load(Relaxed),
            FREE_SIZE_64K_128K.load(Relaxed),
            FREE_SIZE_128K_256K.load(Relaxed),
            FREE_SIZE_256K_512K.load(Relaxed),
            FREE_SIZE_512K_1M.load(Relaxed),
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
}
#[cfg(not(feature = "heap-stats"))]
mod inner {
    pub struct HeapStats;
    pub type BrzMalloc = jemallocator::Jemalloc;
    pub fn heap() -> Option<super::HeapStats> {
        None
    }
    pub fn memtrack() -> String {
        "unknown".to_string()
    }
}
