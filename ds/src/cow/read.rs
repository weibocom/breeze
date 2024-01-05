use std::ops::Deref;
use std::sync::Arc;
use std::{
    hint,
    sync::atomic::{AtomicPtr, AtomicUsize, Ordering::*},
};
pub struct CowReadHandle<T> {
    pub(crate) inner: Arc<CowHandleInner<T>>,
}

impl<T> Clone for CowReadHandle<T> {
    fn clone(&self) -> Self {
        match self {
            CowReadHandle { inner } => CowReadHandle {
                inner: inner.clone(),
            },
        }
    }
}

impl<T> CowReadHandle<T> {
    pub fn copy(&self) -> T
    where
        T: Clone,
    {
        self.get().deref().clone()
    }
    pub fn get(&self) -> ReadGuard<T> {
        self.inner.get()
    }
}

impl<T> From<T> for CowReadHandle<T> {
    fn from(t: T) -> Self {
        let t = Box::into_raw(Box::new(Arc::new(t)));
        Self {
            inner: Arc::new(CowHandleInner {
                inner: AtomicPtr::new(t),
                enters: AtomicUsize::new(0),
                _t: Default::default(),
            }),
        }
    }
}

/// 效果相当于一个Cow<Arc<T>>, 但是
/// - 并发更新会以最后更新的为准，但是没验证过
/// - Cow通过AtomicPtr实现，每次更新T，都会在堆上创建一个Arc<T>，阻塞等到没有读后drop旧Arc<T>
/// - 读取会获取一个对当前堆上Arc<T>的一个clone，否则我们drop后，T将会失效
/// 也就是多个线程获取的T是同一个T，行为本质上和多个线程操作Arc<T>没有区别，不是线程安全的
/// 所以只有T本身是sync+send的时候，我们才是sync+send的，PhantomData保证了这一点
pub(crate) struct CowHandleInner<T> {
    inner: AtomicPtr<Arc<T>>,
    enters: AtomicUsize,
    _t: std::marker::PhantomData<Arc<T>>,
}

#[derive(Clone)]
pub struct ReadGuard<T>(Arc<T>);
impl<T> std::ops::Deref for ReadGuard<T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl<T> CowHandleInner<T> {
    #[inline]
    pub(super) fn get(&self) -> ReadGuard<T> {
        self.enters.fetch_add(1, Release);
        let t = unsafe { &*self.inner.load(Acquire) };
        let new = t.clone();
        self.enters.fetch_sub(1, Release);
        ReadGuard(new)
    }
    pub(super) fn update(&self, t: T) {
        let w_handle = Box::into_raw(Box::new(Arc::new(t)));
        let old = self.inner.swap(w_handle, AcqRel);
        //old有可能被enter load了，这时候释放会有问题，需要等到一次读为0后释放，后续再有读也会是对new的引用，释放old不会再有问题
        while self.enters.load(Acquire) > 0 {
            hint::spin_loop();
        }
        let _dropping = unsafe { Box::from_raw(old) };
    }
}

impl<T> Drop for CowHandleInner<T> {
    fn drop(&mut self) {
        unsafe {
            let _dropping = Box::from_raw(self.inner.load(Acquire));
        }
    }
}
