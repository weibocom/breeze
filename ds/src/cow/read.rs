use std::ptr::NonNull;
use std::sync::atomic::{
    AtomicBool, AtomicPtr, AtomicUsize,
    Ordering::{AcqRel, Acquire, Release},
};
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
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
pub struct CowReadHandleInner<T> {
    inner: AtomicPtr<T>,
    enters: AtomicUsize,
    pub(super) epoch: AtomicBool,
    // 先次更新完之后，会把正在处理中的数据存储到dropping中。所有的reader的读请求都迁移到inner之后，就可以安全的删除
    dropping: AtomicPtr<Vec<*mut T>>,
    _t: std::marker::PhantomData<T>,
}
pub struct ReadGuard<'rh, T> {
    inner: &'rh CowReadHandleInner<T>,
}
impl<'rh, T> std::ops::Deref for ReadGuard<'rh, T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe {
            &self
                .inner
                .inner
                .load(Acquire)
                .as_ref()
                .expect("pointer is nil")
        }
    }
}

impl<'rh, T> Drop for ReadGuard<'rh, T> {
    fn drop(&mut self) {
        // 删除dropping
        //println!("drop read guard");
        if self.inner.enters.fetch_sub(1, AcqRel) == 1 {
            self.inner.purge();
        }
    }
}
impl<T> CowReadHandleInner<T> {
    pub fn read<F: Fn(&T) -> R, R>(&self, f: F) -> R {
        f(&self.enter())
    }
    #[inline]
    pub fn get(&self) -> ReadGuard<'_, T> {
        self.enter()
    }
    fn enter(&self) -> ReadGuard<'_, T> {
        self.enters.fetch_add(1, AcqRel);
        ReadGuard { inner: self }
    }
    // 先把原有的数据swap出来，存储到dropping中。所有的reader请求都迁移到inner之后，将dropping中的数据删除。
    // 只在WriteHandler中调用。
    pub(super) fn update(&self, t: T) {
        assert!(self.epoch.load(Acquire));
        // 确保安全, 确保在epoch设置为false之前，不会调用purge
        let w_handle = unsafe { NonNull::new_unchecked(Box::into_raw(Box::new(t))) };
        let old = self.inner.swap(w_handle.as_ptr(), Release);
        unsafe { self.dropping(old) };
    }
    // 用swap来解决并发问题。
    // 1. 先用0把pre swap出来；
    // 2. 把new 与 pre进行合并
    // 3. 再次进行swap，如果swap出来的数据如果不是zero，说明有并发问题。则继续
    unsafe fn dropping(&self, old: *mut T) {
        let mut new = Box::new(vec![old]);
        loop {
            //println!("dropping old:{:?}", old);
            let pre = self.dropping.swap(0 as _, Release);
            if !pre.is_null() {
                let v: Vec<_> = *Box::from_raw(pre);
                for d in v {
                    if !new.contains(&d) {
                        new.push(d);
                    }
                }
            }
            let zero = self.dropping.swap(Box::leak(new) as _, Release);
            if zero.is_null() {
                break;
            }
            //println!("dropping zero:{:?}", zero);
            new = Box::from_raw(zero);
            std::hint::spin_loop();
        }
    }
    // 把dropping的数据转换出来，并且删除
    fn purge(&self) {
        //println!("purge");
        let old = self.dropping.swap(0 as _, Release);
        if !old.is_null() {
            let v: Vec<_> = unsafe { *Box::from_raw(old) };
            for dropping in v {
                self.drop_old(dropping);
            }
        }
    }
    fn drop_old(&self, p: *mut T) {
        if !p.is_null() {
            //println!("drop old");
            let dropping = { unsafe { Box::from_raw(p) } };
            let t: T = *dropping;
            drop(t);
        }
    }
}

impl<T> Drop for CowReadHandleInner<T> {
    fn drop(&mut self) {
        unsafe {
            self.purge();
            let _dropping = Box::from_raw(self.inner.load(Acquire));
            drop(_dropping);
        }
    }
}
