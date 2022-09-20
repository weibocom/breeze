use super::CowReadHandle;
use std::sync::atomic::Ordering::{AcqRel, Acquire};
pub struct CowWriteHandle<T> {
    r_handle: CowReadHandle<T>,
}
impl<T: Clone> CowWriteHandle<T> {
    pub(crate) fn from(r_handle: CowReadHandle<T>) -> Self {
        Self { r_handle }
    }
    // 如果上一次write请求还未结束，则进入spin状态.
    pub fn write<F: FnMut(&mut T)>(&mut self, mut f: F) {
        let mut t: T = self.r_handle.read(|t| t.clone());
        f(&mut t);
        self.update(t);
    }
    #[inline]
    pub fn update(&mut self, t: T) {
        // lock
        self.r_handle
            .epoch
            .compare_exchange(false, true, AcqRel, Acquire)
            .expect("lock failed");

        let guard = self.get();
        // 在r_handle.update中更新epoch为false.
        self.r_handle.update(t);

        self.r_handle
            .epoch
            .compare_exchange(true, false, AcqRel, Acquire)
            .expect("unlock failed");

        drop(guard);
    }
    #[inline]
    pub fn copy(&self) -> T {
        self.r_handle.read(|t| t.clone())
    }
    #[inline]
    pub fn get(&self) -> super::ReadGuard<'_, T> {
        self.r_handle.get()
    }
}
