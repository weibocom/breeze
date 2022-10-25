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

pub trait Single {
    fn single(&self) -> bool;
    fn disable_single(&self);
    fn enable_single(&self);
}

impl<T> Single for std::sync::Arc<T>
where
    T: Single,
{
    #[inline]
    fn single(&self) -> bool {
        (**self).single()
    }
    #[inline]
    fn disable_single(&self) {
        (**self).disable_single()
    }
    #[inline]
    fn enable_single(&self) {
        (**self).enable_single()
    }
}
