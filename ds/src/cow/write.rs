use super::CowReadHandle;
use std::ops::Deref;
pub struct CowWriteHandle<T> {
    r_handle: CowReadHandle<T>,
}
impl<T> CowWriteHandle<T> {
    pub(crate) fn from(r_handle: CowReadHandle<T>) -> Self {
        Self { r_handle }
    }
    pub fn write<F: FnOnce(&mut T)>(&mut self, f: F)
    where
        T: Clone,
    {
        let mut t: T = self.r_handle.copy();
        f(&mut t);
        self.update(t);
    }
    #[inline]
    pub fn update(&mut self, t: T) {
        //目前自身不能clone，以及mut才能调用update，即使自身是sync的，也没有为多线程提供可变性，说明不存在并发调用的问题
        self.r_handle.inner.update(t);
    }
}

impl<T> Deref for CowWriteHandle<T> {
    type Target = CowReadHandle<T>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.r_handle
    }
}
