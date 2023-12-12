// 不要轻易变更这里面的测试用例，除非你知道你在做什么。拉相关同学进行方案评审。
use std::mem::size_of;

use protocol::{callback::CallbackContext, Parser};
use stream::{Backend, BackendInner, Request};
type Endpoint = Backend<Request>;
type Topology = endpoint::TopologyProtocol<Endpoint, Request, Parser>;
//type RefreshTopology = endpoint::RefreshTopology<Topology>;

type CheckedTopology = stream::CheckedTopology<Topology>;

type CopyBidirectional = stream::pipeline::CopyBidirectional<Stream, Parser, CheckedTopology>;

type Stream = rt::Stream<tokio::net::TcpStream>;
type Handler<'r> = stream::handler::Handler<'r, Request, Parser, Stream>;

type CacheService = endpoint::cacheservice::topo::CacheService<Endpoint, Request, Parser>;
type RedisService = endpoint::redisservice::topo::RedisService<Endpoint, Request, Parser>;
type PhantomService = endpoint::phantomservice::topo::PhantomService<Endpoint, Request, Parser>;
type MsgQue = endpoint::msgque::topo::MsgQue<Endpoint, Request, Parser>;

use rt::Entry;

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
    assert_eq!(16, size_of::<metrics::Metric>());
    assert_eq!(64, size_of::<metrics::Item>());
    assert_eq!(1, size_of::<Parser>());
    assert_eq!(64, size_of::<BackendInner<Request>>());
    assert_eq!(40, size_of::<CheckedTopology>());
    assert_eq!(368, size_of::<stream::StreamMetrics>());
    assert_eq!(24, size_of::<sharding::hash::Hasher>());
}

// 如果要验证 layout-min模式，需要 --features layout-min --release --no-default-features
#[ignore]
#[test]
fn check_layout_rx_buffer() {
    assert_eq!((32, 96).select(), size_of::<rt::TxBuffer>());
}
#[ignore]
#[test]
fn check_callback_ctx() {
    assert_eq!(144, size_of::<CallbackContext>());
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
    assert_eq!((152, 288).select(), size_of::<Stream>());
}
#[ignore]
#[test]
fn check_handler() {
    assert_eq!((216, 368).select(), size_of::<Handler<'static>>());
    assert_eq!(
        (296, 448).select(),
        size_of::<Entry<Handler<'static>, rt::Timeout>>()
    );
}

#[ignore]
#[test]
fn check_topology() {
    assert_eq!(24, size_of::<sharding::hash::Hasher>());
    assert_eq!(96, size_of::<Topology>());
    assert_eq!(72, size_of::<CacheService>());
    assert_eq!(96, size_of::<RedisService>());
    assert_eq!(56, size_of::<PhantomService>());

    assert_eq!(152, size_of::<MsgQue>());
}

#[ignore]
#[test]
fn check_pipeline() {
    assert_eq!((320, 456).select(), size_of::<CopyBidirectional>());
    // 512字节对齐
    assert_eq!(
        (360, 496).select(),
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
