// 不要轻易变更这里面的测试用例，除非你知道你在做什么。拉相关同学进行方案评审。
use std::mem::size_of;

use protocol::{callback::CallbackContext, Parser};
use stream::{Backend, BackendInner, Request};
type Endpoint = Backend<Request>;
type Topology = endpoint::TopologyProtocol<Endpoint, Parser>;
//type RefreshTopology = endpoint::RefreshTopology<Topology>;

type CheckedTopology = stream::CheckedTopology<Topology>;

type CopyBidirectional = stream::pipeline::CopyBidirectional<Stream, Parser, CheckedTopology>;

type Stream = rt::Stream<tokio::net::TcpStream>;
type Handler<'r> = stream::handler::Handler<'r, Request, Parser, Stream>;

type CacheService = endpoint::cacheservice::topo::CacheService<Endpoint, Parser>;
type RedisService = endpoint::redisservice::topo::RedisService<Endpoint, Parser>;
type PhantomService = endpoint::phantomservice::topo::PhantomService<Endpoint, Parser>;
type MsgQue = endpoint::msgque::topo::MsgQue<Endpoint, Parser>;

use rt::{DisableTimeout, Entry};

#[test]
fn checkout_basic() {
    assert_eq!(24, size_of::<ds::RingSlice>());
    assert_eq!(8, size_of::<protocol::Context>());
    assert_eq!(size_of::<protocol::Context>(), 8);
    assert_eq!(
        size_of::<protocol::Context>(),
        size_of::<protocol::kv::Context>()
    );
    assert_eq!(size_of::<protocol::StreamContext>(), 16);
    assert_eq!(
        size_of::<protocol::StreamContext>(),
        size_of::<protocol::redis::RequestContext>()
    );
    assert_eq!(16, size_of::<protocol::Flag>());
    assert_eq!(1, size_of::<protocol::Resource>());
    //assert_eq!(56, size_of::<ds::queue::PinnedQueue<AtomicU32>>());
    assert_eq!(8, size_of::<metrics::Metric>());
    assert_eq!(64, size_of::<metrics::Item>());
    assert_eq!(1, size_of::<Parser>());
    assert_eq!(64, size_of::<BackendInner<Request>>());
    assert_eq!(40, size_of::<CheckedTopology>());
    assert_eq!(192, size_of::<stream::StreamMetrics>());
    assert_eq!(24, size_of::<sharding::hash::Hasher>());
}

// 如果要验证 layout-min模式，需要 --features layout-min --release --no-default-features
#[ignore]
#[test]
fn check_layout_rx_buffer() {
    assert_eq!(32, size_of::<rt::TxBuffer>());
}
#[ignore]
#[test]
fn check_callback_ctx() {
    assert_eq!(192, size_of::<CallbackContext>());
    //assert_eq!(16, size_of::<protocol::callback::Context>());
}
//#[ignore]
//#[test]
//fn check_stream_guard() {
//    assert_eq!((152, 216).select(), size_of::<StreamGuard>());
//}
#[ignore]
#[test]
fn check_stream() {
    assert_eq!(160, size_of::<Stream>());
}
#[ignore]
#[test]
fn check_handler() {
    assert_eq!(216, size_of::<Handler<'static>>());
    assert_eq!(296, size_of::<Entry<Handler<'static>, rt::Timeout>>());
}

#[ignore]
#[test]
fn check_topology() {
    assert_eq!(24, size_of::<sharding::hash::Hasher>());
    assert_eq!(952, size_of::<Topology>());
    assert_eq!(72, size_of::<CacheService>());
    assert_eq!(96, size_of::<RedisService>());
    assert_eq!(56, size_of::<PhantomService>());

    assert_eq!(152, size_of::<MsgQue>());
}

#[ignore]
#[test]
fn check_pipeline() {
    assert_eq!(320, size_of::<CopyBidirectional>());
    // 512字节对齐
    assert_eq!(360, size_of::<Entry<CopyBidirectional, DisableTimeout>>());
}
