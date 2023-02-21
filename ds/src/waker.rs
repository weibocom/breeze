//pub type AtomicWaker = atomic_waker::AtomicWaker;

use std::sync::atomic::{AtomicUsize, Ordering::*};
use std::task::Waker;

#[derive(Default)]
pub struct AtomicWaker {
    state: AtomicUsize,
    waker: Option<Waker>,
}

const TAKEN: usize = 1 << 63;

impl AtomicWaker {
    #[inline]
    pub fn register(&mut self, waker: &Waker) {
        assert!(self.waker.is_none());
        self.waker = Some(waker.clone());
    }
    #[inline]
    pub fn wake(&self) {
        if let Ok(_) = self.state.compare_exchange(0, 1, AcqRel, Relaxed) {
            self.waker
                .as_ref()
                .expect("waker must be registered")
                .wake_by_ref();
        }
    }
    #[inline]
    pub fn waking(&self) -> bool {
        (self.state.load(Acquire) & !TAKEN) > 0
    }
    #[inline]
    pub fn clear(&self) {
        if self.state.load(Acquire) > 0 {
            // 把第0位清0
            self.state.fetch_and(!1, Release);
        }
    }
    #[inline]
    pub fn take(&self) {
        // 把最高位设置为1，表示已经被take了
        self.state.fetch_or(TAKEN, AcqRel);
    }
}
