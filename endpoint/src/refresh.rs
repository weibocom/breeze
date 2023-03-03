use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::{Endpoint, Topology, TopologyCheck};
use discovery::{TopologyRead, TopologyReadGuard};
use sharding::hash::{Hash, HashKey};

// 支持刷新
pub struct RefreshTopology<T> {
    reader: TopologyReadGuard<T>,
    top: AtomicPtr<Arc<DropLog<T>>>,
    updating: AtomicBool,
    gets: AtomicUsize,
    cycle: AtomicUsize,
}
impl<T: Clone + 'static> RefreshTopology<T> {
    // reader一定是已经初始化过的，否则会UB
    #[inline]
    pub fn from(reader: TopologyReadGuard<T>) -> Self {
        let cycle = AtomicUsize::new(reader.cycle());
        let top = Arc::new(reader.do_with(|t| t.clone()).into());
        let top = Box::leak(Box::new(top));
        let top = AtomicPtr::new(top);
        let gets = AtomicUsize::new(0);
        let updating = AtomicBool::new(false);
        Self {
            top,
            reader,
            gets,
            updating,
            cycle,
        }
    }
    pub fn build(self: &Arc<Self>) -> Option<CheckedTopology<T>> {
        self.get().map(|top| CheckedTopology {
            cycle: self.cycle(),
            top,
            inner: self.clone(),
        })
    }
    #[inline]
    fn get(&self) -> Option<Arc<DropLog<T>>> {
        let mut top = None;
        if !self.updating() {
            self.gets.fetch_add(1, Ordering::AcqRel);
            // double check
            if !self.updating() {
                top = Some(unsafe { &*self.top.load(Ordering::Acquire) }.clone());
            }
            self.gets.fetch_sub(1, Ordering::AcqRel);
        }
        top
    }
    #[inline]
    fn update(&self) {
        let cycle = self.cycle();
        if cycle < self.reader.cycle() {
            if self.enable_updating() {
                if self.gets() == 0 {
                    // 没有get。准备更新
                    // 先同步cycle
                    self.set_cycle(self.reader.cycle());
                    let new = Arc::new(self.reader.do_with(|t| t.clone()).into());
                    let new = Box::leak(Box::new(new));
                    let old = self.top.swap(new, Ordering::AcqRel);
                    assert!(!old.is_null());
                    // 直接释放是安全的。因为这是个Arc
                    let _drop = unsafe { Box::from_raw(old) };
                    log::warn!(
                        "top updated. cycle:{} => {} top:{} => {}",
                        cycle,
                        self.reader.cycle(),
                        new as *const _ as usize,
                        old as *const _ as usize
                    );
                }
                self.disable_updating();
            }
        }
    }
    fn set_cycle(&self, c: usize) {
        self.cycle.store(c, Ordering::Release);
    }
    fn cycle(&self) -> usize {
        self.cycle.load(Ordering::Acquire)
    }
    fn updating(&self) -> bool {
        self.updating.load(Ordering::Acquire)
    }
    fn enable_updating(&self) -> bool {
        self.updating
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }
    fn disable_updating(&self) -> bool {
        self.updating
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Relaxed)
            .expect("lock failed")
    }
    fn gets(&self) -> usize {
        self.gets.load(Ordering::Acquire)
    }
}

pub struct CheckedTopology<T> {
    cycle: usize,
    top: Arc<DropLog<T>>,
    inner: Arc<RefreshTopology<T>>,
}
impl<T: Endpoint + Clone + 'static> Endpoint for CheckedTopology<T> {
    type Item = T::Item;
    #[inline]
    fn send(&self, req: T::Item) {
        self.top.send(req);
    }

    #[inline]
    fn shard_idx(&self, hash: i64) -> usize {
        self.top.shard_idx(hash)
    }
}
impl<T: Topology + Clone + 'static> Topology for CheckedTopology<T> {
    #[inline]
    fn exp_sec(&self) -> u32 {
        self.top.exp_sec()
    }
    #[inline]
    fn hash<K: HashKey>(&self, k: &K) -> i64 {
        self.top.hash(k)
    }
}
impl<T: Topology + Clone + 'static> TopologyCheck for CheckedTopology<T> {
    #[inline]
    fn check(&mut self) -> Option<Self> {
        if self.cycle < self.inner.reader.cycle() {
            self.inner.update();
            self.inner.build()
        } else {
            None
        }
    }
}
impl<T: Topology + Clone + 'static> Hash for CheckedTopology<T> {
    #[inline(always)]
    fn hash<S: HashKey>(&self, key: &S) -> i64 {
        self.top.hash(key)
    }
}

unsafe impl<T> Send for RefreshTopology<T> {}
unsafe impl<T> Sync for RefreshTopology<T> {}
unsafe impl<T> Send for CheckedTopology<T> {}
unsafe impl<T> Sync for CheckedTopology<T> {}

#[repr(transparent)]
struct DropLog<T> {
    t: T,
}
impl<T> Drop for DropLog<T> {
    #[inline]
    fn drop(&mut self) {
        log::info!("top dropped {}", self as *const _ as usize);
    }
}
impl<T> From<T> for DropLog<T> {
    fn from(t: T) -> Self {
        Self { t }
    }
}
use std::ops::Deref;
impl<T> Deref for DropLog<T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.t
    }
}

impl<T> Drop for RefreshTopology<T> {
    fn drop(&mut self) {
        let old = self.top.swap(0 as *mut _, Ordering::AcqRel);
        unsafe { Box::from_raw(old) };
    }
}
