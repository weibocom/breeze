use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
use std::sync::Arc;
#[derive(Clone)]
pub struct CowReadHandle<T> {
    inner: Arc<CowReadHandleInner<T>>,
}
impl<T> From<T> for CowReadHandle<T> {
    fn from(t: T) -> Self {
        let t = Box::into_raw(Box::new(t));
        Self {
            inner: Arc::new(CowReadHandleInner {
                inner: AtomicPtr::from(t),
                enters: AtomicUsize::new(0),
                epoch: AtomicBool::new(false),
                dropping: AtomicPtr::default(),
                _t: Default::default(),
            }),
        }
    }
}

impl<T> std::ops::Deref for CowReadHandle<T> {
    type Target = CowReadHandleInner<T>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
pub struct CowReadHandleInner<T> {
    inner: AtomicPtr<T>,
    enters: AtomicUsize,
    pub(crate) epoch: AtomicBool,
    // 先次更新完之后，会把正在处理中的数据存储到dropping中。所有的reader的读请求都迁移到inner之后，就可以安全的删除
    dropping: AtomicPtr<T>,
    _t: std::marker::PhantomData<T>,
}
pub struct ReadGuard<'rh, T> {
    inner: &'rh CowReadHandle<T>,
}
impl<'rh, T> std::ops::Deref for ReadGuard<'rh, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        unsafe {
            &self
                .inner
                .inner
                .inner
                .load(Ordering::Acquire)
                .as_ref()
                .expect("pointer is nil")
        }
    }
}

impl<'rh, T> Drop for ReadGuard<'rh, T> {
    fn drop(&mut self) {
        // 删除dropping
        if self.inner.enters.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.inner.take();
        }
    }
}
impl<T> CowReadHandle<T> {
    pub fn read<F: Fn(&T) -> R, R>(&self, f: F) -> R {
        f(&self.enter())
    }
    #[inline]
    pub fn get(&self) -> ReadGuard<'_, T> {
        self.enter()
    }
    fn enter(&self) -> ReadGuard<'_, T> {
        self.enters.fetch_add(1, Ordering::AcqRel);
        ReadGuard { inner: self }
    }
    // 先把原有的数据swap出来，存储到dropping中。所有的reader请求都迁移到inner之后，将dropping中的数据删除。
    // 在ReadGuard中处理
    pub(crate) fn update(&self, t: T) {
        debug_assert!(self.epoch.load(Ordering::Acquire));
        let w_handle = unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(t))) };
        let old = self.inner.inner.swap(w_handle.as_ptr(), Ordering::Release);
        let dropped = self.dropping.swap(old, Ordering::Release);
        self.drop_old(dropped);
        // 确保安全
        let guard = self.enter();
        self.epoch.store(false, Ordering::Release);
        drop(guard);
    }
    // 把dropping的数据转换出来，并且删除
    fn take(&self) {
        let empty = 0 as *mut T;
        let old = self.dropping.swap(empty, Ordering::Release);
        self.drop_old(old);
    }
    fn drop_old(&self, p: *mut T) {
        if !p.is_null() {
            let dropping = { unsafe { Box::from_raw(p) } };
            let t: T = *dropping;
            drop(t);
        }
    }
}

impl<T> Drop for CowReadHandleInner<T> {
    fn drop(&mut self) {
        unsafe {
            let _dropping = Box::from_raw(self.dropping.load(Ordering::Acquire));
            let _dropping = Box::from_raw(self.inner.load(Ordering::Acquire));
        }
    }
}
