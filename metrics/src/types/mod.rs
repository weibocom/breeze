mod host;
mod number;
mod qps;
mod ratio;
mod rtt;
mod status;

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
        fn incr(&self);
        fn incr_by(&self, v: i64);
        fn decr_by(&self, v: i64);
    }
    impl Adder for AtomicI64 {
        #[inline(always)]
        fn incr(&self) {
            self.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        #[inline(always)]
        fn incr_by(&self, v: i64) {
            self.fetch_add(v, std::sync::atomic::Ordering::Relaxed);
        }
        #[inline(always)]
        fn decr_by(&self, v: i64) {
            self.fetch_sub(v, std::sync::atomic::Ordering::Relaxed);
        }
    }
    use std::sync::atomic::AtomicI64;
    pub static P_W_CACHE: AtomicI64 = AtomicI64::new(0);
    pub static BUF_TX: AtomicI64 = AtomicI64::new(0);
    pub static BUF_RX: AtomicI64 = AtomicI64::new(0);
    pub static POLL_READ: AtomicI64 = AtomicI64::new(0);
    pub static POLL_WRITE: AtomicI64 = AtomicI64::new(0);
    pub static POLL_PENDING_R: AtomicI64 = AtomicI64::new(0);
    pub static POLL_PENDING_W: AtomicI64 = AtomicI64::new(0);
    pub static REENTER_10MS: AtomicI64 = AtomicI64::new(0);
}
