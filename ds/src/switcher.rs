use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

#[derive(Clone)]
pub struct Switcher {
    inner: Arc<AtomicBool>,
}
impl From<bool> for Switcher {
    #[inline(always)]
    fn from(state: bool) -> Self {
        Self {
            inner: Arc::new(AtomicBool::new(state)),
        }
    }
}

impl Switcher {
    #[inline(always)]
    pub fn get(&self) -> bool {
        self.inner.load(Ordering::Acquire)
    }
    #[inline(always)]
    pub fn off(&self) {
        self.inner.store(false, Ordering::Release);
    }
    #[inline(always)]
    pub fn on(&self) {
        self.inner.store(true, Ordering::Release);
    }
}
