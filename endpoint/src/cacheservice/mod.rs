mod config;
mod topology;
use config::Namespace;

pub use topology::Topology;

use discovery::ServiceDiscover;

use protocol::chan::{
    AsyncGetSync, AsyncMultiGet, AsyncOperation, AsyncRoute, AsyncSetSync, AsyncWriteAll,
    PipeToPingPongChanWrite,
};
use protocol::memcache::{Memcache, MemcacheMetaStream, MemcacheOpRoute};
use protocol::DefaultHasher;

use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;

use tokio::net::tcp::OwnedWriteHalf;

use stream::{Cid, RingBufferStream};

type Backend = stream::BackendStream<Arc<RingBufferStream>, Cid>;

type MemcacheRoute = protocol::memcache::MemcacheRoute<DefaultHasher>;
type GetOperation = AsyncRoute<Backend, MemcacheRoute>;
type MultiGetOperation = AsyncMultiGet<Backend, Memcache<DefaultHasher>>;

type Reader = AsyncRoute<Backend, MemcacheRoute>;
type GetThroughOperation = AsyncGetSync<Reader, Memcache<DefaultHasher>>;

type Master = AsyncRoute<Backend, MemcacheRoute>;
type Follower = AsyncRoute<OwnedWriteHalf, MemcacheRoute>;
type StoreOperation = AsyncSetSync<Master, Follower>;
type MetaOperation = MemcacheMetaStream;
type Operation = AsyncOperation<
    GetOperation,
    MultiGetOperation,
    GetThroughOperation,
    StoreOperation,
    MetaOperation,
>;

// 三级访问策略。
// 第一级先进行读写分离
// 第二级按key进行hash
// 第三级进行pipeline与server进行交互
pub struct CacheService<D> {
    inner: PipeToPingPongChanWrite<Memcache<DefaultHasher>, AsyncRoute<Operation, MemcacheOpRoute>>,
    // 第一个元素存储是的op_code
    // 第二个元素存储的是当前op待poll_read的数量
    _mark: std::marker::PhantomData<D>,
}

impl<D> CacheService<D>
where
    D: Unpin,
{
    #[inline]
    fn build_route<S>(shards: Vec<S>) -> AsyncRoute<S, MemcacheRoute>
    where
        S: AsyncWrite + AsyncWriteAll + Unpin,
    {
        let r = MemcacheRoute::from_len(shards.len());
        AsyncRoute::from(shards, r)
    }

    #[inline]
    fn build_routes<S>(pools: Vec<Vec<S>>) -> Vec<AsyncRoute<S, MemcacheRoute>>
    where
        S: AsyncWrite + AsyncWriteAll + Unpin,
    {
        let mut routes = Vec::new();
        for p in pools {
            let r = MemcacheRoute::from_len(p.len());
            routes.push(AsyncRoute::from(p, r));
        }
        routes
    }

    pub async fn from_discovery(discovery: D) -> Result<Self>
    where
        D: ServiceDiscover<super::Topology> + Unpin,
    {
        discovery.do_with(|t| match t {
            Some(t) => match t {
                super::Topology::CacheService(t) => Self::from_topology(t),
                _ => panic!("cacheservice topologyt required!"),
            },
            None => Err(Error::new(
                ErrorKind::ConnectionRefused,
                "backend server not inited yet",
            )),
        })
    }
    fn from_topology(topo: &Topology) -> Result<Self>
    where
        D: ServiceDiscover<super::Topology> + Unpin,
    {
        let get = AsyncOperation::Get(Self::build_route(topo.next_l1()));

        let parser: Memcache<DefaultHasher> = Memcache::<DefaultHasher>::new();
        let l1 = topo.next_l1_gets();
        let gets = AsyncOperation::Gets(AsyncMultiGet::from_shard(l1, parser));

        let master = Self::build_route(topo.master());
        let followers = topo
            .followers()
            .into_iter()
            .map(|shards| Self::build_route(shards))
            .collect();
        let store = AsyncOperation::Store(AsyncSetSync::from_master(master, followers));

        // 获取get through
        //let reads_get_through = Self::build_routes(topo.reader_4_get_through());
        //let parser_get_through: Memcache<DefaultHasher> = Memcache::<DefaultHasher>::new();
        //let get_through =
        //    AsyncOperation::GetThrough(AsyncGetSync::from(reads_get_through, parser_get_through));

        let meta = AsyncOperation::Meta(MemcacheMetaStream::from(""));
        let router = MemcacheOpRoute::new();
        let op_stream = AsyncRoute::from(vec![get, gets, store, meta], router);

        let inner = PipeToPingPongChanWrite::from_stream(Memcache::new(), op_stream);

        Ok(Self {
            inner: inner,
            _mark: Default::default(),
        })
    }
}

use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

impl<D> AsyncRead for CacheService<D>
where
    D: Unpin,
{
    #[inline]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<D> AsyncWrite for CacheService<D>
where
    D: Unpin,
{
    // 支持pipelin.
    // left是表示当前请求还有多少个字节未写入完成
    #[inline]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    #[inline]
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    #[inline]
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
