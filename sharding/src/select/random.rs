use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::Arc;

use crate::SELECTOR_TIMER;
#[derive(Clone)]
pub struct Random<T> {
    idx: Arc<AtomicUsize>,
    current: Arc<AtomicUsize>, // 当前使用的backend使用的时间点
    pub(crate) replicas: Vec<T>,
}

impl<T> Random<T> {
    #[inline]
    pub fn from(replicas: Vec<T>) -> Self {
        assert_ne!(replicas.len(), 0);
        let idx = Arc::new(AtomicUsize::new(rand::random::<u16>() as usize));
        let current = Arc::new(AtomicUsize::new(0));
        Self { idx, current, replicas }
    }
    // 调用方确保replicas的长度至少为1
    #[inline]
    pub unsafe fn unsafe_select(&self) -> (usize, &T) {
        assert_ne!(self.replicas.len(), 0);
        let idx = {
            let t = SELECTOR_TIMER.load(Relaxed);
            if self.current.load(Relaxed) < t {
                self.current.store(t, Relaxed);
                self.idx.fetch_add(1, Relaxed) % self.replicas.len()
            } else {
                self.idx.load(Relaxed) % self.replicas.len()
            }
        };
        assert!(idx < self.replicas.len());
        (idx, self.replicas.get_unchecked(idx))
    }
    #[inline]
    pub unsafe fn unsafe_next(&self, idx: usize) -> (usize, &T) {
        assert_ne!(self.replicas.len(), 0);
        let idx = (idx + 1) % self.replicas.len();
        assert!(idx < self.replicas.len());
        (idx, self.replicas.get_unchecked(idx))
    }
}
