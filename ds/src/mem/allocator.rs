use core::alloc::{GlobalAlloc, Layout};
use jemallocator::Jemalloc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub static ALLOC_SIZE_1_8: AtomicUsize = AtomicUsize::new(0);
pub static ALLOC_SIZE_9_16: AtomicUsize = AtomicUsize::new(0);
pub static ALLOC_SIZE_17_32: AtomicUsize = AtomicUsize::new(0);
pub static ALLOC_SIZE_33_64: AtomicUsize = AtomicUsize::new(0);
pub static ALLOC_SIZE_65_128: AtomicUsize = AtomicUsize::new(0);
pub static ALLOC_SIZE_128_256: AtomicUsize = AtomicUsize::new(0);
pub static ALLOC_SIZE_256_512: AtomicUsize = AtomicUsize::new(0);
pub static ALLOC_SIZE_512_1K: AtomicUsize = AtomicUsize::new(0);
pub static ALLOC_SIZE_1K_2K: AtomicUsize = AtomicUsize::new(0);
pub static ALLOC_SIZE_2K_4K: AtomicUsize = AtomicUsize::new(0);
pub static ALLOC_SIZE_4K_8K: AtomicUsize = AtomicUsize::new(0);
pub static ALLOC_SIZE_8K_16K: AtomicUsize = AtomicUsize::new(0);
pub static ALLOC_SIZE_16K_32K: AtomicUsize = AtomicUsize::new(0);
pub static ALLOC_SIZE_32K_64K: AtomicUsize = AtomicUsize::new(0);
pub static ALLOC_SIZE_64K_128K: AtomicUsize = AtomicUsize::new(0);
pub static ALLOC_SIZE_128K_256K: AtomicUsize = AtomicUsize::new(0);
pub static ALLOC_SIZE_256K_512K: AtomicUsize = AtomicUsize::new(0);
pub static ALLOC_SIZE_512K_1M: AtomicUsize = AtomicUsize::new(0);
pub static ALLOC_SIZE_1M_2M: AtomicUsize = AtomicUsize::new(0);
pub static ALLOC_SIZE_2M_4M: AtomicUsize = AtomicUsize::new(0);
pub static ALLOC_SIZE_4M_8M: AtomicUsize = AtomicUsize::new(0);
pub static ALLOC_SIZE_8M_16M: AtomicUsize = AtomicUsize::new(0);
pub static ALLOC_SIZE_16M_32M: AtomicUsize = AtomicUsize::new(0);
pub static ALLOC_SIZE_32M_64M: AtomicUsize = AtomicUsize::new(0);
pub static ALLOC_SIZE_64M_MORE: AtomicUsize = AtomicUsize::new(0);
pub static ALLOC_TOTAL: AtomicUsize = AtomicUsize::new(0);

pub static FREE_SIZE_1_8: AtomicUsize = AtomicUsize::new(0);
pub static FREE_SIZE_9_16: AtomicUsize = AtomicUsize::new(0);
pub static FREE_SIZE_17_32: AtomicUsize = AtomicUsize::new(0);
pub static FREE_SIZE_33_64: AtomicUsize = AtomicUsize::new(0);
pub static FREE_SIZE_65_128: AtomicUsize = AtomicUsize::new(0);
pub static FREE_SIZE_128_256: AtomicUsize = AtomicUsize::new(0);
pub static FREE_SIZE_256_512: AtomicUsize = AtomicUsize::new(0);
pub static FREE_SIZE_512_1K: AtomicUsize = AtomicUsize::new(0);
pub static FREE_SIZE_1K_2K: AtomicUsize = AtomicUsize::new(0);
pub static FREE_SIZE_2K_4K: AtomicUsize = AtomicUsize::new(0);
pub static FREE_SIZE_4K_8K: AtomicUsize = AtomicUsize::new(0);
pub static FREE_SIZE_8K_16K: AtomicUsize = AtomicUsize::new(0);
pub static FREE_SIZE_16K_32K: AtomicUsize = AtomicUsize::new(0);
pub static FREE_SIZE_32K_64K: AtomicUsize = AtomicUsize::new(0);
pub static FREE_SIZE_64K_128K: AtomicUsize = AtomicUsize::new(0);
pub static FREE_SIZE_128K_256K: AtomicUsize = AtomicUsize::new(0);
pub static FREE_SIZE_256K_512K: AtomicUsize = AtomicUsize::new(0);
pub static FREE_SIZE_512K_1M: AtomicUsize = AtomicUsize::new(0);
pub static FREE_SIZE_1M_2M: AtomicUsize = AtomicUsize::new(0);
pub static FREE_SIZE_2M_4M: AtomicUsize = AtomicUsize::new(0);
pub static FREE_SIZE_4M_8M: AtomicUsize = AtomicUsize::new(0);
pub static FREE_SIZE_8M_16M: AtomicUsize = AtomicUsize::new(0);
pub static FREE_SIZE_16M_32M: AtomicUsize = AtomicUsize::new(0);
pub static FREE_SIZE_32M_64M: AtomicUsize = AtomicUsize::new(0);
pub static FREE_SIZE_64M_MORE: AtomicUsize = AtomicUsize::new(0);
pub static FREE_TOTAL: AtomicUsize = AtomicUsize::new(0);

pub struct BrzMalloc;

unsafe impl GlobalAlloc for BrzMalloc {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let n = layout.size();
        ALLOC_TOTAL.fetch_add(n, Ordering::Relaxed);
        match n {
            1..=8 => ALLOC_SIZE_1_8.fetch_add(n, Ordering::Relaxed),
            9..=16 => ALLOC_SIZE_9_16.fetch_add(n, Ordering::Relaxed),
            17..=32 => ALLOC_SIZE_17_32.fetch_add(n, Ordering::Relaxed),
            33..=64 => ALLOC_SIZE_33_64.fetch_add(n, Ordering::Relaxed),
            65..=128 => ALLOC_SIZE_65_128.fetch_add(n, Ordering::Relaxed),
            129..=256 => ALLOC_SIZE_128_256.fetch_add(n, Ordering::Relaxed),
            257..=512 => ALLOC_SIZE_256_512.fetch_add(n, Ordering::Relaxed),
            513..=1024 => ALLOC_SIZE_512_1K.fetch_add(n, Ordering::Relaxed),
            1025..=2048 => ALLOC_SIZE_1K_2K.fetch_add(n, Ordering::Relaxed),
            2049..=4096 => ALLOC_SIZE_2K_4K.fetch_add(n, Ordering::Relaxed),
            4097..=8192 => ALLOC_SIZE_4K_8K.fetch_add(n, Ordering::Relaxed),
            8193..=16384 => ALLOC_SIZE_8K_16K.fetch_add(n, Ordering::Relaxed),
            16385..=32768 => ALLOC_SIZE_16K_32K.fetch_add(n, Ordering::Relaxed),
            32769..=65536 => ALLOC_SIZE_32K_64K.fetch_add(n, Ordering::Relaxed),
            65537..=131072 => ALLOC_SIZE_64K_128K.fetch_add(n, Ordering::Relaxed),
            131073..=262144 => ALLOC_SIZE_128K_256K.fetch_add(n, Ordering::Relaxed),
            262145..=524288 => ALLOC_SIZE_256K_512K.fetch_add(n, Ordering::Relaxed),
            524289..=1048576 => ALLOC_SIZE_512K_1M.fetch_add(n, Ordering::Relaxed),
            1048577..=2097152 => ALLOC_SIZE_1M_2M.fetch_add(n, Ordering::Relaxed),
            2097153..=4194304 => ALLOC_SIZE_2M_4M.fetch_add(n, Ordering::Relaxed),
            4194305..=8388608 => ALLOC_SIZE_4M_8M.fetch_add(n, Ordering::Relaxed),
            8388609..=16777216 => ALLOC_SIZE_8M_16M.fetch_add(n, Ordering::Relaxed),
            16777217..=33554432 => ALLOC_SIZE_16M_32M.fetch_add(n, Ordering::Relaxed),
            33554433..=67108864 => ALLOC_SIZE_32M_64M.fetch_add(n, Ordering::Relaxed),
            _ => ALLOC_SIZE_64M_MORE.fetch_add(n, Ordering::Relaxed),
        };

        Jemalloc.alloc(layout)
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, _layout: Layout) {
        let n = _layout.size();
        FREE_TOTAL.fetch_add(n, Ordering::Relaxed);
        match n {
            1..=8 => FREE_SIZE_1_8.fetch_add(n, Ordering::Relaxed),
            9..=16 => FREE_SIZE_9_16.fetch_add(n, Ordering::Relaxed),
            17..=32 => FREE_SIZE_17_32.fetch_add(n, Ordering::Relaxed),
            33..=64 => FREE_SIZE_33_64.fetch_add(n, Ordering::Relaxed),
            65..=128 => FREE_SIZE_65_128.fetch_add(n, Ordering::Relaxed),
            129..=256 => FREE_SIZE_128_256.fetch_add(n, Ordering::Relaxed),
            257..=512 => FREE_SIZE_256_512.fetch_add(n, Ordering::Relaxed),
            513..=1024 => FREE_SIZE_512_1K.fetch_add(n, Ordering::Relaxed),
            1025..=2048 => FREE_SIZE_1K_2K.fetch_add(n, Ordering::Relaxed),
            2049..=4096 => FREE_SIZE_2K_4K.fetch_add(n, Ordering::Relaxed),
            4097..=8192 => FREE_SIZE_4K_8K.fetch_add(n, Ordering::Relaxed),
            8193..=16384 => FREE_SIZE_8K_16K.fetch_add(n, Ordering::Relaxed),
            16385..=32768 => FREE_SIZE_16K_32K.fetch_add(n, Ordering::Relaxed),
            32769..=65536 => FREE_SIZE_32K_64K.fetch_add(n, Ordering::Relaxed),
            65537..=131072 => FREE_SIZE_64K_128K.fetch_add(n, Ordering::Relaxed),
            131073..=262144 => FREE_SIZE_128K_256K.fetch_add(n, Ordering::Relaxed),
            262145..=524288 => FREE_SIZE_256K_512K.fetch_add(n, Ordering::Relaxed),
            524289..=1048576 => FREE_SIZE_512K_1M.fetch_add(n, Ordering::Relaxed),
            1048577..=2097152 => FREE_SIZE_1M_2M.fetch_add(n, Ordering::Relaxed),
            2097153..=4194304 => FREE_SIZE_2M_4M.fetch_add(n, Ordering::Relaxed),
            4194305..=8388608 => FREE_SIZE_4M_8M.fetch_add(n, Ordering::Relaxed),
            8388609..=16777216 => FREE_SIZE_8M_16M.fetch_add(n, Ordering::Relaxed),
            16777217..=33554432 => FREE_SIZE_16M_32M.fetch_add(n, Ordering::Relaxed),
            33554433..=67108864 => FREE_SIZE_32M_64M.fetch_add(n, Ordering::Relaxed),
            _ => FREE_SIZE_64M_MORE.fetch_add(n, Ordering::Relaxed),
        };

        Jemalloc.dealloc(ptr, _layout)
    }
}
