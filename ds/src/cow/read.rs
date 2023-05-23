use std::sync::Arc;
use std::{
    hint,
    sync::atomic::{
        AtomicBool, AtomicPtr, AtomicUsize,
        Ordering::{AcqRel, Acquire, Release},
    },
};
#[derive(Clone)]
pub struct CowReadHandle<T> {
    inner: Arc<CowReadHandleInner<T>>,
}
impl<T> From<T> for CowReadHandle<T> {
    fn from(t: T) -> Self {
        let t = Arc::into_raw(Arc::new(t)) as *mut T;
        Self {
            inner: Arc::new(CowReadHandleInner {
                inner: AtomicPtr::new(t),
                released: AtomicBool::new(false),
                enters: AtomicUsize::new(0),
                epoch: AtomicBool::new(false),
                // dropping: AtomicPtr::default(),
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

/// 效果相当于一个Mutex<Cow<Arc<T>>>, 但是
/// - 实际上不会并发更新
/// - Cow通过AtomicPtr实现，每次更新T，都会在堆上创建一个Arc<T>，阻塞等到没有读后drop旧Arc<T>
/// - 读取会获取一个对当前堆上Arc<T>的一个clone，否则我们drop后，T将会失效
/// 也就是多个线程获取的T是同一个T，行为本质上和多个线程操作Arc<T>没有区别，不是线程安全的
pub struct CowReadHandleInner<T> {
    inner: AtomicPtr<T>,
    enters: AtomicUsize,
    released: AtomicBool,
    pub(super) epoch: AtomicBool,
    // 先次更新完之后，会把正在处理中的数据存储到dropping中。所有的reader的读请求都迁移到inner之后，就可以安全的删除
    // dropping: AtomicPtr<Vec<*mut T>>,
    // 此处有疑问？
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

impl<T: Clone> CowReadHandleInner<T> {
    // pub fn read<F: Fn(Arc<T>) -> R, R>(&self, f: F) -> R {
    //     f(self.get())
    // }
    #[inline]
    pub fn get(&self) -> ReadGuard<T> {
        self.enters.fetch_add(1, AcqRel);
        let t = unsafe { Arc::from_raw(self.inner.load(Acquire)) };
        let new = t.clone();
        let old = self.enters.fetch_sub(1, AcqRel);
        //读者数量达到过一次0， 大部分情况下release都是true，没必要store
        if old == 1 && !self.released.load(Acquire) {
            self.released.store(true, Release);
        }
        //自身持有的不能释放
        let _ = Arc::into_raw(t);
        ReadGuard(new)
    }
    pub fn copy(&self) -> T {
        T::clone(&self.get())
    }

    pub(super) fn update(&self, t: T) {
        assert!(self.epoch.load(Acquire));
        let w_handle = Arc::into_raw(Arc::new(t)) as *mut T;
        let old = self.inner.swap(w_handle, Release);
        let _dropping = unsafe { Arc::from_raw(old) };
        //old有可能被enter load了，这时候释放会有问题，需要等到一次读为0后释放，后续再有读也会是对new的引用，释放old不会再有问题
        //released用来标识当enters不等于0时，可能也在swap后达到过一次0，可能对短链接场景有优化
        self.released.store(false, Release);
        while self.enters.load(Acquire) > 0 && !self.released.load(Acquire) {
            hint::spin_loop();
        }
    }
    // 用swap来解决并发问题。
    // 1. 先用0把pre swap出来；
    // 2. 把new 与 pre进行合并
    // 3. 再次进行swap，如果swap出来的数据如果不是zero，说明有并发问题。则继续
    // unsafe fn dropping(&self, old: *mut T) {
    //     let mut new = Box::new(vec![old]);
    //     loop {
    //         //println!("dropping old:{:?}", old);
    //         let pre = self.dropping.swap(0 as _, Release);
    //         if !pre.is_null() {
    //             let v: Vec<_> = *Box::from_raw(pre);
    //             for d in v {
    //                 if !new.contains(&d) {
    //                     new.push(d);
    //                 }
    //             }
    //         }
    //         let zero = self.dropping.swap(Box::leak(new) as _, Release);
    //         if zero.is_null() {
    //             break;
    //         }
    //         //println!("dropping zero:{:?}", zero);
    //         new = Box::from_raw(zero);
    //         std::hint::spin_loop();
    //     }
    // }
    // 把dropping的数据转换出来，并且删除
    // fn purge(&self) {
    //     //println!("purge");
    //     let old = self.dropping.swap(0 as _, Release);
    //     if !old.is_null() {
    //         let v: Vec<_> = unsafe { *Box::from_raw(old) };
    //         for dropping in v {
    //             self.drop_old(dropping);
    //         }
    //     }
    // }
    // fn drop_old(&self, p: *mut T) {
    //     if !p.is_null() {
    //         //println!("drop old");
    //         let dropping = { unsafe { Box::from_raw(p) } };
    //         let t: T = *dropping;
    //         drop(t);
    //     }
    // }
}

impl<T> Drop for CowReadHandleInner<T> {
    fn drop(&mut self) {
        unsafe {
            let _dropping = Arc::from_raw(self.inner.load(Acquire));
        }
    }
}
