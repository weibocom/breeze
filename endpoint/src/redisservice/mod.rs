mod config;
mod topo;

use std::collections::hash_map::Entry;

use std::collections::HashMap;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;

use discovery::TopologyRead;
use protocol::Protocol;
use stream::{
    Addressed, AsyncLayerGet, AsyncMultiGetSharding, AsyncOpRoute, AsyncOperation, AsyncSetSync,
    AsyncSharding, LayerRole, LayerRoleAble, MetaStream, SeqLoadBalance,
};

pub use config::RedisNamespace;
use std::pin::Pin;
use std::task::{Context, Poll};
use protocol::redis::Command::Gets;
use stream::{AsyncReadAll, AsyncWriteAll, Request, Response};
pub use topo::RedisTopology;

use crate::Topology;

type Backend = stream::BackendStream;

type GetOperation<P> = AsyncLayerGet<
    AsyncSharding<SeqLoadBalance<Backend>, P>,
    AsyncSharding<SeqLoadBalance<Backend>, P>,
    P,
>;

type MultiGetLayer<P> = AsyncMultiGetSharding<SeqLoadBalance<Backend>, P>;
type MultiGetOperation<P> = AsyncLayerGet<MultiGetLayer<P>, AsyncSharding<SeqLoadBalance<Backend>, P>, P>;
type Master<P> = AsyncSharding<Backend, P>;
type Follower<P> = AsyncSharding<Backend, P>;
type StoreOperation<P> = AsyncSetSync<Master<P>, Follower<P>, P>;
type MetaOperation<P> = MetaStream<P, Backend>;
type Operation<P> =
    AsyncOperation<GetOperation<P>, MultiGetOperation<P>, StoreOperation<P>, MetaOperation<P>>;

pub struct RedisService<P> {
    inner: AsyncOpRoute<Operation<P>>,
}

impl<P> RedisService<P> {
    pub async fn from_discovery<D>(p: P, discovery: D) -> Result<Self>
    where
        D: TopologyRead<Topology<P>>,
        P: protocol::Protocol,
    {
        discovery.do_with(|t| match t {
            Topology::RedisTopo(r) => return Self::from_topology::<D>(p.clone(), r),
            _ => {
                log::warn!("malformed redis discovery");
                Err(Error::new(
                    ErrorKind::InvalidInput,
                    "malformed discovery for redis",
                ))
            }
        })
    }
    fn from_topology<D>(p: P, topo: &RedisTopology<P>) -> Result<Self>
    where
        D: TopologyRead<Topology<P>>,
        P: protocol::Protocol,
    {
        // 初始化完成一定会保障master存在，并且长度不为0.
        use discovery::Inited;
        use AsyncOperation::*;
        assert!(topo.inited());
        let hash = topo.hash();
        let dist = topo.distribution();

        let master = AsyncSharding::from(LayerRole::Master, topo.master(), hash, dist, p.clone());
        let store = Store(AsyncSetSync::from_master(master, Vec::new(), p.clone()));

        let get_layers = build_get(topo.slaves(), hash, dist, p.clone());
        let get_padding_layer: Vec<AsyncSharding<SeqLoadBalance<Backend>, P>> = vec![];
        let get = Get(AsyncLayerGet::from_layers(
            get_layers,
            get_padding_layer,
            p.clone(),
        ));

        let multi_get_layers = build_mget(topo.slaves(), p.clone(), hash, dist);
        let multi_get_padding_layer: Vec<AsyncSharding<SeqLoadBalance<Backend>, P>> = vec![];
        let multi_get = MGet(AsyncLayerGet::from_layers(
            multi_get_layers,
            multi_get_padding_layer,
            p.clone()
        ));

        let mut operations = HashMap::with_capacity(4);
        operations.insert(protocol::Operation::Get, get);
        operations.insert(protocol::Operation::MGet, multi_get);
        operations.insert(protocol::Operation::Store, store);
        let mut alias = HashMap::new();
        alias.insert(protocol::Operation::Meta, protocol::Operation::Store);
        let op_stream = AsyncOpRoute::from(operations, alias);

        log::debug!(
            "redis server logic connection established:{:?}",
            op_stream.addr()
        );

        Ok(Self { inner: op_stream })
    }
}

impl<P> AsyncReadAll for RedisService<P>
where
    P: Protocol,
{
    #[inline]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Response>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

impl<P> AsyncWriteAll for RedisService<P>
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
fn build_get<S, P>(
    pools: Vec<(LayerRole, Vec<S>)>,
    h: &str,
    distribution: &str,
    parser: P,
) -> Vec<AsyncSharding<SeqLoadBalance<S>, P>>
where
    S: AsyncWriteAll + Addressed + Unpin,
    P: Protocol + Clone,
{
    let load_balance = build_load_balance(pools);
    let load_balance_merged = merge_by_layer(load_balance);
    let sharding = build_sharding(load_balance_merged, h, distribution, parser);
    sharding
}

#[inline]
fn merge_by_layer<S>(pools: Vec<S>) -> Vec<(LayerRole, Vec<S>)>
where
    S: LayerRoleAble + Addressed,
{
    let mut layer_role_map: HashMap<LayerRole, Vec<S>> = HashMap::with_capacity(pools.len());
    for pool in pools {
        let single_layer: &mut Vec<S> = match layer_role_map.entry(pool.layer_role().clone()) {
            Entry::Occupied(o) => o.into_mut(),
            Entry::Vacant(v) => v.insert(Vec::new()),
        };

        single_layer.push(pool);
    }
    let layer_role_vec = layer_role_map
        .into_iter()
        .map(|(key, value)| (key, value))
        .collect();

    layer_role_vec
}

#[inline]
fn build_sharding<S, P>(
    pools: Vec<(LayerRole, Vec<S>)>,
    h: &str,
    distribution: &str,
    parser: P,
) -> Vec<AsyncSharding<S, P>>
where
    S: AsyncWriteAll + Addressed,
    P: Protocol + Clone,
{
    let mut sharding: Vec<AsyncSharding<S, P>> = Vec::with_capacity(pools.len());
    for (role, p) in pools {
        sharding.push(AsyncSharding::from(
            role,
            p,
            h,
            distribution,
            parser.clone(),
        ))
    }
    sharding
}

#[inline]
fn build_load_balance<S>(
    pools: Vec<(LayerRole, Vec<S>)>,
) -> Vec<SeqLoadBalance<S>>
where
    S: AsyncWriteAll + Addressed,
{
    let mut load_balance: Vec<SeqLoadBalance<S>> = Vec::with_capacity(pools.len());
    for (role, p) in pools {
        load_balance.push(SeqLoadBalance::from(role.clone(), p))
    }

    load_balance
}

#[inline]
fn build_multi_get_sharding<S, P>(
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

#[inline]
fn build_mget<S,P> (
    pools: Vec<(LayerRole, Vec<S>)>,
    parser: P,
    hash: &str,
    distribution: &str,
) -> Vec<AsyncMultiGetSharding<SeqLoadBalance<S>, P>>
    where
        S: AsyncWriteAll + Addressed + Unpin,
        P: Protocol + Clone,
{
    let load_balance = build_load_balance(pools);
    let load_balance_merged = merge_by_layer(load_balance);
    let sharding = build_multi_get_sharding(load_balance_merged, parser, hash, distribution);
    sharding
}
