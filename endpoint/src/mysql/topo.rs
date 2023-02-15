use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use discovery::TopologyWrite;
use ds::time::Duration;
use protocol::Protocol;
use protocol::Request;
use sharding::distribution::Distribute;
use sharding::hash::Hasher;
use sharding::ReplicaSelect;
use sharding::Selector;

use crate::Builder;
use crate::Single;
use crate::{Endpoint, Topology};

#[derive(Clone)]
pub struct MysqlService<B, E, Req, P> {
    // 默认后端分片，一共shards.len()个分片，每个分片 shard[0]是master, shard[1..]是slave
    direct_shards: Vec<Shard<E>>,
    // 默认不同sharding的url。第0个是master
    direct_shards_url: Vec<Vec<String>>,

    // 按时间维度分库分表
    archive_shards: HashMap<String, Vec<Shard<E>>>,
    archive_shards_url: HashMap<String, Vec<Vec<String>>>,

    hasher: Hasher,
    distribute: Distribute,
    selector: Selector, // 从的选择策略。
    updated: HashMap<String, Arc<AtomicBool>>,
    parser: P,
    service: String,
    timeout_master: Duration,
    timeout_slave: Duration,
    _mark: std::marker::PhantomData<(B, Req)>,
}

impl<B, E, Req, P> From<P> for MysqlService<B, E, Req, P> {
    #[inline]
    fn from(parser: P) -> Self {
        Self {
            parser,
            direct_shards: Default::default(),
            direct_shards_url: Default::default(),
            archive_shards: Default::default(),
            archive_shards_url: Default::default(),
            hasher: Default::default(),
            distribute: Default::default(),
            updated: Default::default(),
            service: Default::default(),
            selector: Selector::Random,
            timeout_master: crate::TO_REDIS_M,
            timeout_slave: crate::TO_REDIS_S,
            _mark: Default::default(),
        }
    }
}

impl<B, E, Req, P> Topology for MysqlService<B, E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
    B: Send + Sync,
{
    fn hasher(&self) -> &sharding::hash::Hasher {
        &self.hasher
    }
}

impl<B: Send + Sync, E, Req, P> Endpoint for MysqlService<B, E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
{
    type Item = Req;

    fn send(&self, req: Self::Item) {}

    fn shard_idx(&self, hash: i64) -> usize {
        self.distribute.index(hash)
    }
}

impl<B, E, Req, P> TopologyWrite for MysqlService<B, E, Req, P>
where
    B: Builder<P, Req, E>,
    P: Protocol,
    E: Endpoint<Item = Req> + Single,
{
    fn need_load(&self) -> bool {
        false
    }
    fn load(&mut self) {}
    fn update(&mut self, name: &str, cfg: &str) {}
}
#[derive(Clone)]
struct Shard<E> {
    master: (String, E),
    slaves: ReplicaSelect<(String, E)>,
}
