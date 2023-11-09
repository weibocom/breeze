mod buffer;
pub use buffer::*;

mod resized;
pub use resized::*;

mod ring_slice;
pub use ring_slice::*;

mod guarded;
pub use guarded::*;

mod policy;
pub use policy::*;

mod malloc;
pub use malloc::*;

pub mod arena;

mod bytes;
pub use self::bytes::*;

use std::sync::atomic::{AtomicI64, Ordering::Relaxed};
pub static BUF_TX: Buffers = Buffers::new();
pub static BUF_RX: Buffers = Buffers::new();
pub static CACHE_ALLOC_NUM: AtomicI64 = AtomicI64::new(0);
pub static CACHE_MISS_ALLOC_NUM: AtomicI64 = AtomicI64::new(0);

pub struct Buffers {
    pub num: AtomicI64,         // 字节数
    pub cnt: AtomicI64,         // buffer的数量
    pub num_alloc: AtomicI64,   // 分配的数量. 用于计算num per sec
    pub bytes_alloc: AtomicI64, // 分配的字节数. 用于计算 bytes / secs
    pub layouts: [AtomicI64; 16],
}
impl Buffers {
    #[inline]
    pub const fn new() -> Self {
        let layouts: [AtomicI64; 16] = [
            AtomicI64::new(0),
            AtomicI64::new(0),
            AtomicI64::new(0),
            AtomicI64::new(0),
            AtomicI64::new(0),
            AtomicI64::new(0),
            AtomicI64::new(0),
            AtomicI64::new(0),
            AtomicI64::new(0),
            AtomicI64::new(0),
            AtomicI64::new(0),
            AtomicI64::new(0),
            AtomicI64::new(0),
            AtomicI64::new(0),
            AtomicI64::new(0),
            AtomicI64::new(0),
        ];
        Self {
            layouts,
            num: AtomicI64::new(0),
            cnt: AtomicI64::new(0),
            num_alloc: AtomicI64::new(0),
            bytes_alloc: AtomicI64::new(0),
        }
    }
    #[inline]
    pub fn incr_by(&self, v: usize) {
        self.bytes_alloc.fetch_add(v as i64, Relaxed);
        self.num_alloc.fetch_add(1, Relaxed);
        self.num.fetch_add(v as i64, Relaxed);
        self.cnt.fetch_add(1, Relaxed);
        self.layouts[self.idx(v)].fetch_add(1, Relaxed);
    }
    #[inline]
    pub fn decr_by(&self, v: usize) {
        self.num.fetch_sub(v as i64, Relaxed);
        self.cnt.fetch_sub(1, Relaxed);
        self.layouts[self.idx(v)].fetch_sub(1, Relaxed);
    }
    #[inline]
    fn idx(&self, v: usize) -> usize {
        let v = v as usize;
        if v == 0 {
            0
        } else {
            assert!(v.is_power_of_two());
            let b = v / 2048;
            //#[cfg(not(debug_assertions))]
            //assert!(b.is_power_of_two(), "{} not valid", v);
            (1 + b.trailing_zeros()).min(15) as usize
        }
    }
}
