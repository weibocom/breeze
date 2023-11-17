mod host;
mod number;
mod qps;
mod ratio;
mod rtt;
mod status;

pub(crate) use host::*;
pub use host::{decr_task, incr_task, resource_num_metric, set_sockfile_failed};
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
