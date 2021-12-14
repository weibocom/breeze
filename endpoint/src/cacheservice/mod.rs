mod config;
mod topo;

pub use config::MemcacheNamespace;
use std::collections::HashMap;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;

use discovery::TopologyRead;
use protocol::Protocol;
use stream::{
    Addressed, AsyncLayerGet, AsyncMultiGetSharding, AsyncOpRoute, AsyncOperation, AsyncSetSync,
    AsyncSharding, LayerRole, MetaStream,
};
pub use topo::MemcacheTopology;

type Backend = stream::BackendStream;
type GetOperation<P> = AsyncLayerGet<AsyncSharding<Backend, P>, AsyncSharding<Backend, P>, P>;
type MultiGetLayer<P> = AsyncMultiGetSharding<Backend, P>;
type MultiGetOperation<P> = AsyncLayerGet<MultiGetLayer<P>, AsyncSharding<Backend, P>, P>;
type Master<P> = AsyncSharding<Backend, P>;
type Follower<P> = AsyncSharding<Backend, P>;
type StoreOperation<P> = AsyncSetSync<Master<P>, Follower<P>, P>;
type MetaOperation<P> = MetaStream<P, Backend>;
type Operation<P> =
    AsyncOperation<GetOperation<P>, MultiGetOperation<P>, StoreOperation<P>, MetaOperation<P>>;

// 三级访问策略。
// 第一级先进行读写分离
// 第二级按key进行hash
// 第三级进行pipeline与server进行交互
pub struct CacheService<P> {
    inner: AsyncOpRoute<Operation<P>>,
}

impl<P> CacheService<P> {
    pub async fn from_discovery<D>(p: P, discovery: D) -> Result<Self>
    where
        D: TopologyRead<Topology<P>>,
        P: protocol::Protocol,
    {
        discovery.do_with(|t| match t {
            Topology::CacheService(mem_topo) => {
                return Self::from_topology::<D>(p.clone(), mem_topo);
            }
            _ => {
                log::warn!("malformed discovery for cacheservice");
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "malformed cacheservice discovery",
                ));
            }
        })
    }
    fn from_topology<D>(p: P, topo: &MemcacheTopology<P>) -> Result<Self>
    where
        P: protocol::Protocol,
    {
        // 初始化完成一定会保障master存在，并且长度不为0.
        use discovery::Inited;
        use AsyncOperation::*;
        assert!(topo.inited());

        let hash = topo.hash();
        let dist = topo.distribution();
        let (streams, write_back) = topo.mget();
        let mget_layers = build_mget(streams, p.clone(), hash, dist);
        let mget_layers_writeback = build_layers(write_back, hash, dist, p.clone());
        let mget = MGet(AsyncLayerGet::from_layers(
            mget_layers,
            mget_layers_writeback,
            p.clone(),
        ));

        let master = AsyncSharding::from(LayerRole::Master, topo.master(), hash, dist, p.clone());
        let noreply = build_layers(topo.followers(), hash, dist, p.clone());
        let store = Store(AsyncSetSync::from_master(master, noreply, p.clone()));

        // 获取get through
        let (streams, write_back) = topo.get();
        let get_layers = build_layers(streams, hash, dist, p.clone());
        let get_layers_writeback = build_layers(write_back, hash, dist, p.clone());
        let get = Get(AsyncLayerGet::from_layers(
            get_layers,
            get_layers_writeback,
            p.clone(),
        ));

        // meta与master共享一个物理连接。
        //let meta = Meta(MetaStream::from(p.clone(), topo.master()));
        let mut operations = HashMap::with_capacity(4);
        operations.insert(protocol::Operation::Get, get);
        operations.insert(protocol::Operation::MGet, mget);
        operations.insert(protocol::Operation::Store, store);
        // operations.insert(protocol::Operation::Meta, meta);
        let mut alias = HashMap::new();
        alias.insert(protocol::Operation::Meta, protocol::Operation::Store);
        let op_stream = AsyncOpRoute::from(operations, alias);

        log::debug!("cs logic connection established:{:?}", op_stream.addr());

        Ok(Self { inner: op_stream })
    }
}

use std::pin::Pin;
use std::task::{Context, Poll};

use stream::{AsyncReadAll, AsyncWriteAll, Request, Response};

use crate::Topology;

impl<P> AsyncReadAll for CacheService<P>
where
    P: Protocol,
{
    #[inline]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Response>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

impl<P> AsyncWriteAll for CacheService<P>
where
    P: Protocol,
{
    #[inline]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &Request,
    ) -> Poll<Result<()>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
}

#[inline]
fn build_layers<S, P>(
    pools: Vec<(LayerRole, Vec<S>)>,
    h: &str,
    distribution: &str,
    parser: P,
) -> Vec<AsyncSharding<S, P>>
where
    S: AsyncWriteAll + Addressed,
    P: Protocol + Clone,
{
    let mut layers: Vec<AsyncSharding<S, P>> = Vec::with_capacity(pools.len());
    for (role, p) in pools {
        layers.push(AsyncSharding::from(
            role,
            p,
            h,
            distribution,
            parser.clone(),
        ));
    }
    layers
}

#[inline]
fn build_mget<S, P>(
    pools: Vec<(LayerRole, Vec<S>)>,
    parser: P,
    h: &str,
    d: &str,
) -> Vec<AsyncMultiGetSharding<S, P>>
where
    S: AsyncWriteAll + Addressed,
    P: Clone,
{
    let mut layers: Vec<AsyncMultiGetSharding<S, P>> = Vec::with_capacity(pools.len());
    for (role, p) in pools {
        layers.push(AsyncMultiGetSharding::from_shard(
            role,
            p,
            parser.clone(),
            h,
            d,
        ));
    }
    layers
}
