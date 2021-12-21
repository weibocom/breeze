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
    pub fn write<F: FnMut(&mut T)>(&mut self, mut f: F)
    where
        T: Clone,
    {
        // 待上一次write请求结束
        let mut spin = 0usize;
        while let Err(_) =
            self.r_handle
                .epoch
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
        {
            spin += 1;
            std::hint::spin_loop();
        }
        //  fence(Ordering::Acquire);
        if spin > 0 {
            log::info!("cow spins {} times.", spin);
        }
        let mut t: T = self.r_handle.read(|t| t.clone());
        f(&mut t);
        self.r_handle.update(t);
    }
    pub fn copy(&self) -> T
    where
        T: Clone,
    {
        self.r_handle.read(|t| t.clone())
    }
    pub fn get(&self) -> crate::ReadGuard<'_, T> {
        self.r_handle.get()
    }
}
