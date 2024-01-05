mod host;
mod number;
mod qps;
mod ratio;
mod rtt;
mod status;

use crate::MetricType;

pub(crate) use host::*;
pub use host::{decr_task, incr_task, set_sockfile_failed};
pub(crate) use number::*;
pub(crate) use qps::*;
pub(crate) use ratio::*;
pub use rtt::MAX;
pub(crate) use rtt::*;
pub(crate) use status::*;

pub mod base {
    pub trait Adder {
        #[inline(always)]
        fn incr(&self) {
            self.incr_by(1);
        }
        fn incr_by(&self, v: i64);
        #[inline(always)]
        fn decr(&self) {
            self.decr_by(1);
        }
        fn decr_by(&self, v: i64);
        fn take(&self) -> i64;
        fn get(&self) -> i64;
        fn set(&self, v: i64);
        #[inline(always)]
        fn max(&self, v: i64) {
            let cur = self.get();
            if cur < v {
                self.set(v);
            }
        }
    }
    impl Adder for AtomicI64 {
        #[inline(always)]
        fn incr_by(&self, v: i64) {
            self.fetch_add(v, Relaxed);
        }
        #[inline(always)]
        fn decr_by(&self, v: i64) {
            self.fetch_sub(v, Relaxed);
        }
        #[inline(always)]
        fn get(&self) -> i64 {
            self.load(Relaxed)
        }
        #[inline(always)]
        fn set(&self, v: i64) {
            self.store(v, Relaxed);
        }
        #[inline(always)]
        fn take(&self) -> i64 {
            let v = self.get();
            self.decr_by(v);
            v
        }
    }
    use std::sync::atomic::{AtomicI64, Ordering::Relaxed};
    pub static P_W_CACHE: AtomicI64 = AtomicI64::new(0);
    pub static POLL_READ: AtomicI64 = AtomicI64::new(0);
    pub static POLL_WRITE: AtomicI64 = AtomicI64::new(0);
    pub static POLL_PENDING_R: AtomicI64 = AtomicI64::new(0);
    pub static POLL_PENDING_W: AtomicI64 = AtomicI64::new(0);
    pub static REENTER_10MS: AtomicI64 = AtomicI64::new(0);
    pub static LEAKED_CONN: AtomicI64 = AtomicI64::new(0);
}

use crate::ItemWriter as Writer;
use enum_dispatch::enum_dispatch;
use std::sync::atomic::AtomicI64;
#[enum_dispatch]
pub(crate) trait Snapshot {
    fn snapshot<W: Writer>(&self, path: &str, key: &str, data: &ItemData, w: &mut W, secs: f64);
    #[inline]
    fn merge(&self, global: &ItemData, cache: &ItemData) {
        let _ = global;
        let _ = cache;
    }
    fn is_empty(&self, data: &ItemData) -> bool {
        use crate::base::Adder;
        data.d0.get() == 0 && data.d1.get() == 0
    }
}
// 用4个i64来存储数据。
#[derive(Default, Debug)]
pub(crate) struct ItemData {
    d0: AtomicI64,
    d1: AtomicI64,
    d2: AtomicI64,
    d3: AtomicI64,
}

pub(crate) trait IncrTo {
    fn incr_to(&self, data: &ItemData);
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub struct Empty;
impl IncrTo for Empty {
    #[inline]
    fn incr_to(&self, _data: &ItemData) {}
}
impl Snapshot for Empty {
    #[inline]
    fn snapshot<W: Writer>(&self, _: &str, _: &str, _: &ItemData, _: &mut W, _: f64) {}
}
