mod config;
mod topology;
use config::Namespace;

pub use topology::Topology;

use discovery::ServiceDiscover;

use hash::Hasher;
use stream::{
    AsyncGetSync, AsyncMultiGet, AsyncMultiGetSharding, AsyncOperation, AsyncRoute, AsyncSetSync,
    AsyncSharding, MetaStream,
};

use protocol::Protocol;

use std::io::{Error, ErrorKind, Result};

type Backend = stream::BackendStream;

// type GetOperation<P> = AsyncSharding<Backend, Hasher, P>;
type GetLayer<P> = AsyncSharding<Backend, Hasher, P>;
type GetOperation<P> = AsyncGetSync<GetLayer<P>, P>;

type MultiGetLayer<P> = AsyncMultiGetSharding<Backend, P>;
type MultiGetOperation<P> = AsyncMultiGet<MultiGetLayer<P>, P>;

type Master<P> = AsyncSharding<Backend, Hasher, P>;
type Follower<P> = AsyncSharding<Backend, Hasher, P>;
type StoreOperation<P> = AsyncSetSync<Master<P>, Follower<P>>;
type MetaOperation<P> = MetaStream<P, Backend>;
type Operation<P> =
    AsyncOperation<GetOperation<P>, MultiGetOperation<P>, StoreOperation<P>, MetaOperation<P>>;

// 三级访问策略。
// 第一级先进行读写分离
// 第二级按key进行hash
// 第三级进行pipeline与server进行交互
pub struct CacheService<P> {
    inner: AsyncRoute<Operation<P>, P>,
}

impl<P> CacheService<P> {
    #[inline]
    fn build_sharding<S>(shards: Vec<S>, h: &str, parser: P) -> AsyncSharding<S, Hasher, P>
    where
        S: AsyncWriteAll,
    {
        let hasher = Hasher::from(h);
        AsyncSharding::from(shards, hasher, parser)
    }

    #[inline]
    fn build_get_layers<S>(
        pools: Vec<Vec<S>>,
        h: &str,
        parser: P,
    ) -> Vec<AsyncSharding<S, Hasher, P>>
    where
        S: AsyncWriteAll,
        P: Clone,
    {
        let mut layers: Vec<AsyncSharding<S, Hasher, P>> = Vec::new();
        for p in pools {
            let hasher = Hasher::from(h);
            layers.push(AsyncSharding::from(p, hasher, parser.clone()));
        }
        layers
    }

    #[inline]
    fn build_get_multi_layers<S>(pools: Vec<Vec<S>>, parser: P) -> Vec<AsyncMultiGetSharding<S, P>>
    where
        S: AsyncWriteAll,
        P: Clone,
    {
        let mut layers: Vec<AsyncMultiGetSharding<S, P>> = Vec::new();
        for p in pools {
            layers.push(AsyncMultiGetSharding::from_shard(p, parser.clone()));
        }
        layers
    }

    pub async fn from_discovery<D>(p: P, discovery: D) -> Result<Self>
    where
        D: ServiceDiscover<super::Topology<P>>,
        P: protocol::Protocol,
    {
        discovery.do_with(|t| match t {
            Some(t) => match t {
                super::Topology::CacheService(t) => Self::from_topology::<D>(p, t),
            },
            None => Err(Error::new(
                ErrorKind::ConnectionRefused,
                "backend server not inited yet",
            )),
        })
    }
    fn from_topology<D>(parser: P, topo: &Topology<P>) -> Result<Self>
    where
        D: ServiceDiscover<super::Topology<P>>,
        P: protocol::Protocol,
    {
        let hash_alg = &topo.hash;
        // let get = AsyncOperation::Get(Self::build_sharding(
        //     topo.next_l1(),
        //     &hash_alg,
        //     parser.clone(),
        // ));

        // let l1 = topo.next_l1_gets();
        let get_multi_layers = Self::build_get_multi_layers(topo.retrive_gets(), parser.clone());
        let get_multi =
            AsyncOperation::Gets(AsyncMultiGet::from_layers(get_multi_layers, parser.clone()));

        let master = Self::build_sharding(topo.master(), &hash_alg, parser.clone());
        let followers = topo
            .followers()
            .into_iter()
            .map(|shards| Self::build_sharding(shards, &hash_alg, parser.clone()))
            .collect();
        let store = AsyncOperation::Store(AsyncSetSync::from_master(master, followers));

        // 获取get through
        let get_layers = Self::build_get_layers(topo.retrive_get(), &hash_alg, parser.clone());
        let get = AsyncOperation::Get(AsyncGetSync::from(get_layers, parser.clone()));

        let all_instances = topo.meta();

        let meta = AsyncOperation::Meta(MetaStream::from(parser.clone(), all_instances));
        let op_stream = AsyncRoute::from(vec![get, get_multi, store, meta], parser.clone());

        Ok(Self { inner: op_stream })
    }
}

use std::pin::Pin;
use std::task::{Context, Poll};

use stream::{AsyncReadAll, AsyncWriteAll, Request, Response};

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
    // 支持pipelin.
    // left是表示当前请求还有多少个字节未写入完成
    #[inline]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &Request,
    ) -> Poll<Result<()>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
}
