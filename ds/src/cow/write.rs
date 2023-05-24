use super::CowReadHandle;
use std::{
    ops::Deref,
    sync::atomic::Ordering::{AcqRel, Acquire},
};
pub struct CowWriteHandle<T> {
    r_handle: CowReadHandle<T>,
}
impl<T: Clone> CowWriteHandle<T> {
    pub(crate) fn from(r_handle: CowReadHandle<T>) -> Self {
        Self { r_handle }
    }
    pub fn write<F: FnOnce(&mut T)>(&mut self, f: F) {
        let mut t: T = self.r_handle.copy();
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

        // let guard = self.get();
        // 在r_handle.update中更新epoch为false.
        self.r_handle.update(t);

        self.r_handle
            .epoch
            .compare_exchange(true, false, AcqRel, Acquire)
            .expect("unlock failed");

        // drop(guard);
    }
}

impl<T> Deref for CowWriteHandle<T> {
    type Target = CowReadHandle<T>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.r_handle
    }
}
