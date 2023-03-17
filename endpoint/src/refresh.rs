use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;

use crate::{Endpoint, Topology, TopologyCheck};
use discovery::{TopologyRead, TopologyReadGuard};
use sharding::hash::{Hash, HashKey};

// 支持刷新
pub struct RefreshTopology<T> {
    reader: TopologyReadGuard<T>,
    top: spin::RwLock<AtomicPtr<Arc<DropLog<T>>>>,
}
impl<T: Clone + 'static> RefreshTopology<T> {
    // reader一定是已经初始化过的，否则会UB
    #[inline]
    pub fn from(reader: TopologyReadGuard<T>) -> Self {
        let top = Arc::new(reader.do_with(|t| t.clone()).into());
        let top = Box::leak(Box::new(top));
        let top = AtomicPtr::new(top).into();
        Self { top, reader }
    }
    pub fn build(self: &Arc<Self>) -> Option<CheckedTopology<T>> {
        // 读取一次cycle，保证更新后的cycle一定大于当前的cycle
        let cycle = self.reader.cycle();
        self._build(cycle)
    }
    pub fn _build(self: &Arc<Self>, cycle: usize) -> Option<CheckedTopology<T>> {
        self.get().map(|top| CheckedTopology {
            cycle,
            top,
            inner: self.clone(),
        })
    }
    #[inline]
    fn get(&self) -> Option<Arc<DropLog<T>>> {
        self.top.try_read().map(|top| {
            let top = unsafe { &*top.load(Ordering::Acquire) }.clone();
            top
        })
    }
    #[inline]
    fn update(&self, cycle: usize, _update_cycle: usize) {
        // 所有的连接都会触发更新。因为需要加写锁
        self.top.try_write().map(|top| {
            let new = Arc::new(self.reader.do_with(|t| t.clone()).into());
            let new = Box::leak(Box::new(new));
            let old = top.swap(new, Ordering::AcqRel);
            assert!(!old.is_null());
            // 直接释放是安全的。因为这是个Arc
            let _drop = unsafe { Box::from_raw(old) };
            log::warn!(
                "top updated. cycle:{} => {}({}) top:{} => {}",
                cycle,
                self.reader.cycle(),
                _update_cycle,
                new as *const _ as usize,
                old as *const _ as usize
            );
        });
    }
}

pub struct CheckedTopology<T> {
    cycle: usize,
    top: Arc<DropLog<T>>,
    inner: Arc<RefreshTopology<T>>,
}
impl<T: Endpoint + Clone + 'static> Endpoint for CheckedTopology<T> {
    type Item = T::Item;
    #[inline(always)]
    fn send(&self, req: T::Item) {
        self.top.send(req);
    }

    #[inline(always)]
    fn shard_idx(&self, hash: i64) -> usize {
        self.top.shard_idx(hash)
    }
}
impl<T: Topology + Clone + 'static> Topology for CheckedTopology<T> {
    #[inline(always)]
    fn exp_sec(&self) -> u32 {
        self.top.exp_sec()
    }
    #[inline(always)]
    fn hash<K: HashKey>(&self, k: &K) -> i64 {
        self.top.hash(k)
    }
}
impl<T: Topology + Clone + 'static> TopologyCheck for CheckedTopology<T> {
    #[inline]
    fn check(&mut self) -> Option<Self> {
        // update_cycle先load出来，避免update过程中cycle变化导致丢失更新
        let update_cycle = self.inner.reader.cycle();
        if self.cycle < update_cycle {
            self.inner.update(self.cycle, update_cycle);
            self.inner._build(update_cycle)
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
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.t
    }
}

impl<T> Drop for RefreshTopology<T> {
    fn drop(&mut self) {
        let top = self.top.try_write().expect("top write lock");
        let old = top.swap(0 as *mut _, Ordering::AcqRel);
        assert!(!old.is_null());
        unsafe { Box::from_raw(old) };
    }
}
