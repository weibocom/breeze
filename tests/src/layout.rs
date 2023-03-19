// 不要轻易变更这里面的测试用例，除非你知道你在做什么。拉相关同学进行方案评审。
use std::{
    mem::size_of,
    sync::{atomic::AtomicU32, Arc},
};

use protocol::{callback::CallbackContext, Parser};
use stream::{Backend, Request};
type Endpoint = Arc<Backend<Request>>;
type Topology = endpoint::TopologyProtocol<Builder, Endpoint, Request, Parser>;
//type RefreshTopology = endpoint::RefreshTopology<Topology>;

type CheckedTopology = endpoint::CheckedTopology<Topology>;

type CopyBidirectional = stream::pipeline::CopyBidirectional<Stream, Parser, CheckedTopology>;

type Stream = rt::Stream<tokio::net::TcpStream>;
type Handler<'r> = stream::handler::Handler<'r, Request, Parser, Stream>;

type Builder = stream::Builder<Parser, Request>;
type CacheService = endpoint::cacheservice::topo::CacheService<Builder, Endpoint, Request, Parser>;
type RedisService = endpoint::redisservice::topo::RedisService<Builder, Endpoint, Request, Parser>;
type PhantomService =
    endpoint::phantomservice::topo::PhantomService<Builder, Endpoint, Request, Parser>;
type MsgQue = endpoint::msgque::topo::MsgQue<Builder, Endpoint, Request, Parser>;

use rt::Entry;

#[test]
fn checkout_basic() {
    assert_eq!(24, size_of::<ds::RingSlice>());
    assert_eq!(8, size_of::<protocol::Context>());
    assert_eq!(
        size_of::<protocol::Context>(),
        size_of::<protocol::redis::RequestContext>()
    );
    assert_eq!(16, size_of::<protocol::Flag>());
    assert_eq!(1, size_of::<protocol::Resource>());
    assert_eq!(56, size_of::<ds::queue::PinnedQueue<AtomicU32>>());
    assert_eq!(16, size_of::<metrics::Metric>());
    assert_eq!(64, size_of::<metrics::Item>());
    assert_eq!(1, size_of::<Parser>());
    assert_eq!(48, size_of::<Backend<Request>>());
    assert_eq!(0, size_of::<Builder>());
    assert_eq!(24, size_of::<CheckedTopology>());
    assert_eq!(368, size_of::<stream::StreamMetrics>());
    assert_eq!(24, size_of::<sharding::hash::Hasher>());
    assert_eq!(32, size_of::<ds::MemGuard>());
}

// 如果要验证 layout-min模式，需要 --features layout-min --release --no-default-features
#[ignore]
#[test]
fn check_layout_rx_buffer() {
    assert_eq!((32, 96).select(), size_of::<rt::TxBuffer>());
}
#[test]
fn check_callback_ctx() {
    assert_eq!(56, size_of::<protocol::HashedCommand>());
    assert_eq!(48, size_of::<protocol::Command>());
    assert_eq!(152, size_of::<CallbackContext>());
}
//#[ignore]
//#[test]
//fn check_stream_guard() {
//    assert_eq!((152, 216).select(), size_of::<StreamGuard>());
//}
#[ignore]
#[test]
fn check_stream() {
    assert_eq!((232, 368).select(), size_of::<Stream>());
}
#[ignore]
#[test]
fn check_handler() {
    assert_eq!((328, 464).select(), size_of::<Handler<'static>>());
    assert_eq!(
        (432, 568).select(),
        size_of::<Entry<Handler<'static>, rt::Timeout>>()
    );
}

#[ignore]
#[test]
fn check_topology() {
    assert_eq!(24, size_of::<sharding::hash::Hasher>());
    assert_eq!(160, size_of::<Topology>());
    assert_eq!(72, size_of::<CacheService>());
    assert_eq!(160, size_of::<RedisService>());
    assert_eq!(120, size_of::<PhantomService>());
    assert_eq!(152, size_of::<MsgQue>());
}

#[ignore]
#[test]
fn check_pipeline() {
    assert_eq!((432, 568).select(), size_of::<CopyBidirectional>());
    // 512字节对齐
    assert_eq!(
        (496, 632).select(),
        size_of::<Entry<CopyBidirectional, rt::DisableTimeout>>()
    );
}

trait Select {
    fn select(&self) -> usize;
}

impl Select for (usize, usize) {
    fn select(&self) -> usize {
        if cfg!(feature = "layout-min") {
            assert!(
                !cfg!(debug_assertions) && !cfg!(feature = "layout-max"),
                "layout-min must be used with release mode"
            );
            self.0
        } else {
            assert!(
                cfg!(debug_assertions) && !cfg!(feature = "layout-min"),
                "layout-max must be used with debug mode"
            );
            self.1
        }
    }
}
