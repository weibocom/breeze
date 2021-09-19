use super::{CowReadHandle, Update};
use std::sync::atomic::Ordering;
pub struct CowWriteHandle<T, O> {
    r_handle: CowReadHandle<T>,
    _op: std::marker::PhantomData<O>,
}
impl<T, O> CowWriteHandle<T, O>
where
    T: Update<O>,
{
    pub(crate) fn from(r_handle: CowReadHandle<T>) -> Self {
        Self {
            r_handle: r_handle,
            _op: Default::default(),
        }
    }
    // 如果上一次write请求还未结束，则进入spin状态.
    pub fn write(&mut self, op: &O)
    where
        T: Update<O> + Clone,
    {
        // 待上一次write请求结束
        while self.r_handle.epoch.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
        let mut t: T = self.r_handle.read(|t| t.clone());
        t.update(op);
        self.r_handle.update(t);
    }
}
