use ds::ReadGuard;

use crate::request::Request;

pub trait Endpoint: Send + Sync {
    fn send(&self, req: Request);
    // 返回hash应该发送的分片idx
    fn shard_idx(&self, _hash: i64) -> usize {
        log::warn!("+++ should not use defatult shard idx");
        panic!("should not use defatult shard idx");
    }
}

impl<T> Endpoint for &T
where
    T: Endpoint,
{
    #[inline]
    fn send(&self, req: Request) {
        (*self).send(req)
    }

    #[inline]
    fn shard_idx(&self, hash: i64) -> usize {
        (*self).shard_idx(hash)
    }
}

impl<T> Endpoint for std::sync::Arc<T>
where
    T: Endpoint,
{
    #[inline]
    fn send(&self, req: Request) {
        (**self).send(req)
    }
    #[inline]
    fn shard_idx(&self, hash: i64) -> usize {
        (**self).shard_idx(hash)
    }
}

impl<T> Endpoint for ReadGuard<T>
where
    T: Endpoint,
{
    #[inline]
    fn send(&self, req: Request) {
        (&self).send(req)
    }
    #[inline]
    fn shard_idx(&self, hash: i64) -> usize {
        (&self).shard_idx(hash)
    }
}
