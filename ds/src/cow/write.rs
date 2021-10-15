use super::CowReadHandle;
use std::sync::atomic::Ordering;
pub struct CowWriteHandle<T> {
    r_handle: CowReadHandle<T>,
}
impl<T> CowWriteHandle<T> {
    pub(crate) fn from(r_handle: CowReadHandle<T>) -> Self {
        Self { r_handle: r_handle }
    }
    // 如果上一次write请求还未结束，则进入spin状态.
    pub fn write<F: Fn(&mut T)>(&mut self, f: F)
    where
        T: Clone,
    {
        // 待上一次write请求结束
        while self.r_handle.epoch.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
        let mut t: T = self.r_handle.read(|t| t.clone());
        f(&mut t);
        self.r_handle.update(t);
    }
}
