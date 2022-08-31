use enum_dispatch::enum_dispatch;

use sharding::hash::Hasher;

pub trait Endpoint: Sized + Send + Sync {
    type Item;
    fn send(&self, req: Self::Item);
    //#[inline]
    //fn static_send(receiver: usize, req: Self::Item) {
    //    let e = unsafe { &*(receiver as *const Self) };
    //    e.send(req);
    //}
    //
    // 返回hash应该发送的分片idx
    fn shard_idx(&self, _hash: i64) -> usize {
        log::warn!("+++ should not use defatult shard idx");
        panic!("should not use defatult shard idx");
    }
}

impl<T, R> Endpoint for &T
where
    T: Endpoint<Item = R>,
{
    type Item = R;
    #[inline]
    fn send(&self, req: R) {
        (*self).send(req)
    }

    #[inline]
    fn shard_idx(&self, hash: i64) -> usize {
        (*self).shard_idx(hash)
    }
}

impl<T, R> Endpoint for std::sync::Arc<T>
where
    T: Endpoint<Item = R>,
{
    type Item = R;
    #[inline]
    fn send(&self, req: R) {
        (**self).send(req)
    }
    #[inline]
    fn shard_idx(&self, hash: i64) -> usize {
        (**self).shard_idx(hash)
    }
}

#[enum_dispatch]
pub trait Topology: Endpoint {
    #[inline]
    fn exp_sec(&self) -> u32 {
        86400
    }
    fn hasher(&self) -> &Hasher;
}

impl<T> Topology for std::sync::Arc<T>
where
    T: Topology,
{
    #[inline]
    fn exp_sec(&self) -> u32 {
        (**self).exp_sec()
    }
    #[inline]
    fn hasher(&self) -> &Hasher {
        (**self).hasher()
    }
}
pub trait TopologyCheck: Sized {
    fn check(&mut self) -> Option<Self>;
}
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub type BorrowRawPtr = usize;
pub type BorrowPtrGuard = Arc<AtomicUsize>;
// 支持callback时，避免循环依赖，使用BorrowPtr。
#[derive(Clone)]
pub struct BorrowPtr<T> {
    ptr: *const T,
    guard: BorrowPtrGuard,
}
impl<T> BorrowPtr<T> {
    #[inline]
    pub fn new(ptr: T) -> Self {
        let ptr = Box::leak(Box::new(ptr)) as *const T;
        Self {
            ptr,
            guard: Arc::new(AtomicUsize::new(1)),
        }
    }
    #[inline]
    pub unsafe fn as_ref(&self) -> &T {
        &*self.ptr
    }
    #[inline]
    pub fn borrow(&self) -> (BorrowRawPtr, BorrowPtrGuard) {
        self.guard.fetch_add(1, Ordering::AcqRel);
        (self.ptr as usize, self.guard.clone())
    }
}
impl<T> Drop for BorrowPtr<T> {
    #[inline]
    fn drop(&mut self) {
        let borrowd = self.guard.fetch_sub(1, Ordering::Relaxed);
        log::debug!("borrowed return:{}", borrowd);
    }
}

pub trait Borrow {
    type Item;
    unsafe fn borrow(&self) -> (*const Self::Item, BorrowPtrGuard);
}

impl<T> Borrow for std::sync::Arc<T>
where
    T: Borrow,
{
    type Item = T::Item;
    #[inline]
    unsafe fn borrow(&self) -> (*const Self::Item, BorrowPtrGuard) {
        (**self).borrow()
    }
}
