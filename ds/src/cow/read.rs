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

/// 效果相当于一个Mutex<Cow<Acr<T>>>, 但是
/// - 锁非阻塞，而是会panic
/// - Cow通过AtomicPtr实现，每次更新T，都会在堆上创建一个Arc<T>，阻塞等到没有读后drop旧Arc<T>
/// - 读取会获取一个对当前堆上Arc<T>的一个clone，否则我们drop后，T将会失效
/// 也就是多个线程获取的T是同一个T，行为本质上和多个线程操作Arc<T>没有区别，不是线程安全的，因此为做出提示，T必须是Send和Sync的
/// 为什么要暴露Arc的实现呢？因为想不到copy on write有单线程使用场景
pub struct CowReadHandleInner<T> {
    inner: AtomicPtr<T>,
    enters: AtomicUsize,
    pub(super) epoch: AtomicBool,
    // 先次更新完之后，会把正在处理中的数据存储到dropping中。所有的reader的读请求都迁移到inner之后，就可以安全的删除
    // dropping: AtomicPtr<Vec<*mut T>>,
    // 此处有疑问？
    _t: std::marker::PhantomData<Arc<T>>,
}

// pub type ReadGuard<T> = Arc<T>;
// pub struct ReadGuard<'rh, T> {
//     inner: &'rh CowReadHandleInner<T>,
// }
// impl<'rh, T> std::ops::Deref for ReadGuard<'rh, T> {
//     type Target = T;
//     #[inline]
//     fn deref(&self) -> &Self::Target {
//         unsafe {
//             &self
//                 .inner
//                 .inner
//                 .load(Acquire)
//                 .as_ref()
//                 .expect("pointer is nil")
//         }
//     }
// }

// impl<'rh, T> Drop for ReadGuard<'rh, T> {
//     fn drop(&mut self) {
//         // 删除dropping
//         //println!("drop read guard");
//         if self.inner.enters.fetch_sub(1, AcqRel) == 1 {
//             self.inner.purge();
//         }
//     }
// }

impl<T: Clone> CowReadHandleInner<T> {
    // pub fn read<F: Fn(Arc<T>) -> R, R>(&self, f: F) -> R {
    //     f(self.get())
    // }
    ///此引用会失效，不可保留，fn一定要轻量级，否则会阻塞更新，重量操作请用get
    pub fn do_with<F: Fn(&T) -> R, R>(&self, f: F) -> R {
        self.enter(|| {
            let t = unsafe { self.inner.load(Acquire).as_ref().unwrap() };
            f(t)
        })
    }
    #[inline]
    pub fn get(&self) -> Arc<T> {
        self.enter(|| unsafe {
            let t = Arc::from_raw(self.inner.load(Acquire));
            let new = t.clone();
            let _ = Arc::into_raw(t);
            new
        })
    }
    #[inline]
    pub fn copy(&self) -> T {
        self.enter(|| unsafe {
            self.inner
                .load(Acquire)
                .as_ref()
                .expect("pointer is nil")
                .clone()
        })
    }

    fn enter<F: Fn() -> R, R>(&self, f: F) -> R {
        self.enters.fetch_add(1, AcqRel);
        let r = f();
        self.enters.fetch_sub(1, AcqRel);
        r
    }
    // 先把原有的数据swap出来，存储到dropping中。所有的reader请求都迁移到inner之后，将dropping中的数据删除。
    // 只在WriteHandler中调用。
    pub(super) fn update(&self, t: T) {
        assert!(self.epoch.load(Acquire));
        let w_handle = Arc::into_raw(Arc::new(t)) as *mut T;
        let old = self.inner.swap(w_handle, Release);
        //old有可能被enter load了，这时候释放会有问题，需要等到一次读为0后释放，后续再有读也会是对new的引用，释放old不会再有问题
        while self.enters.load(Acquire) > 0 {
            hint::spin_loop();
        }
        unsafe { Arc::from_raw(old) };
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
