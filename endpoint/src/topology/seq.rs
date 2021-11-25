use std::sync::atomic::{AtomicUsize, Ordering};

use rand::Rng;

pub struct Seq {
    inner: AtomicUsize,
}
impl std::ops::Deref for Seq {
    type Target = AtomicUsize;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
impl Default for Seq {
    fn default() -> Self {
        Self::random()
    }
}
impl Clone for Seq {
    fn clone(&self) -> Self {
        Self {
            inner: AtomicUsize::new(self.load(Ordering::Acquire)),
        }
    }
}
impl Seq {
    fn random() -> Self {
        // 一般情况下, 一层的sharding数量不会超过64k。
        let rd = rand::thread_rng().gen_range(0..65536);
        Self {
            inner: AtomicUsize::new(rd),
        }
    }
}
