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
use stream::{AsyncReadAll, AsyncWriteAll, Request, Response};
pub use topo::RedisTopology;

use crate::Topology;

type Backend = stream::BackendStream;

type GetOperation<P> = AsyncLayerGet<
    SeqLoadBalance<AsyncSharding<Backend, P>>,
    SeqLoadBalance<AsyncSharding<Backend, P>>,
    P,
>;

type MultiGetLayer<P> = AsyncMultiGetSharding<Backend, P>;
type MultiGetOperation<P> = AsyncLayerGet<MultiGetLayer<P>, AsyncSharding<Backend, P>, P>;
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
            Topology::RedisService(r) => return Self::from_topology::<D>(p.clone(), r),
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
        let get_padding_layer: Vec<SeqLoadBalance<AsyncSharding<Backend, P>>> = vec![];
        let get = Get(AsyncLayerGet::from_layers(
            get_layers,
            get_padding_layer,
            p.clone(),
        ));

        let mut operations = HashMap::with_capacity(4);
        operations.insert(protocol::Operation::Get, get);
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
    // =======
    //     where
    //         D: TopologyRead<super::Topology<P>>,
    //         P: protocol::Protocol,
    //     {
    //         discovery.do_with(|t| Self::from_topology::<D>(p.clone(), t.to_concrete_topo()))
    //     }
    //     fn from_topology<D>(p: P, topo: Box<&dyn ServiceTopo>) -> Result<Self>
    //     where
    //         D: TopologyRead<super::Topology<P>>,
    //         P: protocol::Protocol,
    //     {
    //         // 初始化完成一定会保障master存在，并且长度不为0.
    //         // use discovery::Inited;
    //         use AsyncOperation::*;
    //         assert!(topo.topo_inited());
    //         let hash = topo.hash();
    //         let dist = topo.distribution();

    //         let (get_readers, _) = topo.get();
    //         let get_layers = build_get_layers(get_readers, hash, dist, p.clone());
    //         let get_padding_layer: Vec<AsyncSharding<Backend, P>> = vec![];
    //         let get_op = Get(AsyncLayerGet::from_layers(
    //             get_layers,
    //             get_padding_layer,
    //             p.clone(),
    //         ));

    //         let (mget_readers, _) = topo.mget();
    //         let mget_layers = build_mget_layers(mget_readers, hash, dist, p.clone());
    //         let mget_padding_layer: Vec<AsyncSharding<Backend, P>> = vec![];
    //         let mget_op = MGet(AsyncLayerGet::from_layers(
    //             mget_layers,
    //             mget_padding_layer,
    //             p.clone(),
    //         ));

    //         let store_layer =
    //             AsyncSharding::from(LayerRole::Master, topo.master(), hash, dist, p.clone());
    //         let store_op = Store(AsyncSetSync::from_master(store_layer, vec![], p.clone()));

    //         let mut operations = HashMap::with_capacity(4);
    //         operations.insert(protocol::Operation::Get, get_op);
    //         operations.insert(protocol::Operation::MGet, mget_op);
    //         operations.insert(protocol::Operation::Store, store_op);
    //         let mut alias = HashMap::with_capacity(1);
    //         alias.insert(protocol::Operation::Meta, protocol::Operation::Store);
    //         let op_route = AsyncOpRoute::from(operations, alias);

    //         log::info!("redis logic connection established:{:?}", op_route.addr());

    //         Ok(Self { inner: op_route })
    //     }
    // }

    // use std::pin::Pin;
    // use std::task::{Context, Poll};

    // use stream::{AsyncReadAll, Request, Response};

    // use crate::ServiceTopo;

    // impl<P> AsyncReadAll for RedisService<P>
    // where
    //     P: Protocol,
    // >>>>>>> redis_conn_manage
{
    #[inline]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Response>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

impl<P> AsyncWriteAll for RedisService<P>
// <<<<<<< HEAD
where
    P: Protocol,
    // =======
    // where
    //     P: Protocol,
    // >>>>>>> redis_conn_manage
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
) -> Vec<SeqLoadBalance<AsyncSharding<S, P>>>
where
    S: AsyncWriteAll + Addressed + Unpin,
    P: Protocol + Clone,
{
    let sharding = build_sharding(pools, h, distribution, parser);
    let sharding_merged = merge_by_layer(sharding);
    let load_balance = build_load_balance(sharding_merged);
    load_balance
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

// <<<<<<< HEAD
#[inline]
fn build_load_balance<S>(
    // =======
    // // 对于redis，读写都只请求一层
    // #[inline]
    // fn build_get_layers<S, P>(
    // >>>>>>> redis_conn_manage
    pools: Vec<(LayerRole, Vec<S>)>,
) -> Vec<SeqLoadBalance<S>>
// <<<<<<< HEAD
where
    S: AsyncWriteAll + Addressed,
{
    let mut load_balance: Vec<SeqLoadBalance<S>> = Vec::with_capacity(pools.len());
    for (role, p) in pools {
        load_balance.push(SeqLoadBalance::from(role.clone(), p))

        // =======
        // where
        //     S: AsyncWriteAll + Addressed,
        //     P: Protocol + Clone,
        // {
        //     let mut layers: Vec<AsyncSharding<S, P>> = Vec::with_capacity(pools.len());
        //     for (r, p) in pools {
        //         let layer = AsyncSharding::from(r, p, h, distribution, parser.clone());
        //         layers.push(layer);
        // >>>>>>> redis_conn_manage
    }

    load_balance
}

// <<<<<<< HEAD
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
        // =======
        // // 对于redis，读写都只请求一层
        // #[inline]
        // fn build_mget_layers<S, P>(
        //     pools: Vec<(LayerRole, Vec<S>)>,
        //     h: &str,
        //     distribution: &str,
        //     parser: P,
        // ) -> Vec<AsyncMultiGetSharding<S, P>>
        // where
        //     S: AsyncWriteAll + Addressed,
        //     P: Protocol + Clone,
        // {
        //     let mut layers: Vec<AsyncMultiGetSharding<S, P>> = Vec::with_capacity(pools.len());
        //     for (r, p) in pools {
        //         let layer = AsyncMultiGetSharding::from_shard(r, p, parser.clone(), h, distribution);
        //         layers.push(layer);
        // >>>>>>> redis_conn_manage
    }
    layers
}
