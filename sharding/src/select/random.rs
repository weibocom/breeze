use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone)]
pub struct Random<T> {
    idx: Arc<AtomicUsize>,
    pub(crate) replicas: Vec<T>,
}

impl<T> Random<T> {
    #[inline]
    pub fn from(replicas: Vec<T>) -> Self {
        debug_assert_ne!(replicas.len(), 0);
        let idx = Arc::new(AtomicUsize::new(rand::random::<u16>() as usize));
        Self { idx, replicas }
    }
    // 调用方确保replicas的长度至少为1
    #[inline(always)]
    pub unsafe fn unsafe_select(&self) -> (usize, &T) {
        let idx = self.idx.fetch_add(1, Ordering::Relaxed) % self.replicas.len();
        (idx, self.replicas.get_unchecked(idx))
    }
    #[inline(always)]
    pub unsafe fn unsafe_next(&self, idx: usize) -> (usize, &T) {
        let idx = (idx + 1) % self.replicas.len();
        (idx, self.replicas.get_unchecked(idx))
    }
}
