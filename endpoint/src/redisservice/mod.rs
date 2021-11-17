// <<<<<<< HEAD
use std::collections::HashMap;
use std::io::Result;

use discovery::TopologyRead;
use protocol::Protocol;
use stream::{
    Addressed, AsyncLayerGet, AsyncMultiGetSharding, AsyncOpRoute, AsyncOperation, AsyncSetSync,
    AsyncSharding, LayerRole, MetaStream,
};

use std::pin::Pin;
use std::task::{Context, Poll};

pub use crate::topology::Topology;
use crate::ServiceTopo;
use stream::{AsyncReadAll, AsyncWriteAll, Request, Response};

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

pub struct RedisService<P> {
    inner: AsyncOpRoute<Operation<P>>,
    // =======
    // mod topology;

    // use discovery::TopologyRead;
    // use protocol::Protocol;
    // use std::{collections::HashMap, io::Result};
    // use stream::{
    //     Addressed, AsyncLayerGet, AsyncMultiGetSharding, AsyncOpRoute, AsyncOperation, AsyncSetSync,
    //     AsyncSharding, AsyncWriteAll, LayerRole, MetaStream,
    // };
    // pub use topology::Topology;

    // type Backend = stream::BackendStream;
    // type GetOperation<P> = AsyncLayerGet<AsyncSharding<Backend, P>, AsyncSharding<Backend, P>, P>;
    // type MGetLayer<P> = AsyncMultiGetSharding<Backend, P>;
    // type MGetOperation<P> = AsyncLayerGet<MGetLayer<P>, AsyncSharding<Backend, P>, P>;
    // type Master<P> = AsyncSharding<Backend, P>;
    // // store： 对于MS，没有followers，对于Hash，需要设置followers
    // type StoreOperation<P> = AsyncSetSync<Master<P>, Master<P>, P>;
    // type MetaOperation<P> = MetaStream<P, Backend>;
    // type RedisOperation<P> =
    //     AsyncOperation<GetOperation<P>, MGetOperation<P>, StoreOperation<P>, MetaOperation<P>>;

    // pub struct RedisService<P> {
    //     inner: AsyncOpRoute<RedisOperation<P>>,
    // >>>>>>> redis_conn_manage
}

impl<P> RedisService<P> {
    pub async fn from_discovery<D>(p: P, discovery: D) -> Result<Self>
    // <<<<<<< HEAD
    where
        D: TopologyRead<super::Topology<P>>,
        P: protocol::Protocol,
    {
        discovery.do_with(|t| Self::from_topology::<D>(p.clone(), t))
    }
    fn from_topology<D>(p: P, topo: &Topology<P>) -> Result<Self>
    where
        D: TopologyRead<super::Topology<P>>,
        P: protocol::Protocol,
    {
        // 初始化完成一定会保障master存在，并且长度不为0.
        use discovery::Inited;
        use AsyncOperation::*;
        assert!(topo.inited());
        let hash = topo.hash();
        let dist = topo.distribution();

        let master = AsyncSharding::from(LayerRole::Master, topo.master(), hash, dist, p.clone());
        let slaves = build_layers(topo.followers(), hash, dist, p.clone());
        let store = Store(AsyncSetSync::from_master(master, Vec::new(), p.clone()));

        let get = Get(AsyncLayerGet::from_layers(
            slaves,
            Vec::new(),
            p.clone(),
        ));

        let mut operations = HashMap::with_capacity(4);
        operations.insert(protocol::Operation::Get, get);
        operations.insert(protocol::Operation::Store, store);
        let mut alias = HashMap::new();
        alias.insert(protocol::Operation::Meta, protocol::Operation::Store);
        let op_stream = AsyncOpRoute::from(operations, alias);

        log::debug!("redis server logic connection established:{:?}", op_stream.addr());

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

// <<<<<<< HEAD
#[inline]
fn build_layers<S, P>(
    // =======
    // // 对于redis，读写都只请求一层
    // #[inline]
    // fn build_get_layers<S, P>(
    // >>>>>>> redis_conn_manage
    pools: Vec<(LayerRole, Vec<S>)>,
    h: &str,
    distribution: &str,
    parser: P,
) -> Vec<AsyncSharding<S, P>>
// <<<<<<< HEAD
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
    layers
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
