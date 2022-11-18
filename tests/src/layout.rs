// 不要轻易变更这里面的测试用例，除非你知道你在做什么。拉相关同学进行方案评审。
use std::{
    mem::size_of,
    sync::{atomic::AtomicU32, Arc},
};

use protocol::{callback::CallbackContext, Parser};
use stream::{buffer::StreamGuard, Backend, Request};
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
    assert_eq!(32, size_of::<ds::RingSlice>());
    assert_eq!(8, size_of::<protocol::Context>());
    assert_eq!(
        size_of::<protocol::Context>(),
        size_of::<protocol::redis::RequestContext>()
    );
    assert_eq!(24, size_of::<protocol::Flag>());
    assert_eq!(1, size_of::<protocol::Resource>());
    assert_eq!(56, size_of::<ds::queue::PinnedQueue<AtomicU32>>());
    assert_eq!(16, size_of::<metrics::Metric>());
    assert_eq!(64, size_of::<metrics::Item>());
    assert_eq!(1, size_of::<Parser>());
    assert_eq!(48, size_of::<Backend<Request>>());
    assert_eq!(0, size_of::<Builder>());
    assert_eq!(24, size_of::<CheckedTopology>());
}

// 如果要验证 layout-min模式，需要 --features layout-min --release --no-default-features
#[test]
fn check_layout() {
    assert_eq!((200, 208).select(), size_of::<CallbackContext>());
    assert_eq!((176, 256).select(), size_of::<StreamGuard>());
    assert_eq!((112, 280).select(), size_of::<Stream>());
    assert_eq!((368, 616).select(), size_of::<Handler<'static>>());
    assert_eq!((520, 792).select(), size_of::<Entry<Handler<'static>>>());

    //assert_eq!(400, size_of::<Topology>());
    assert_eq!(96, size_of::<CacheService>());
    assert_eq!(264, size_of::<RedisService>());
    assert_eq!(192, size_of::<PhantomService>());
    assert_eq!(392, size_of::<MsgQue>());

    assert_eq!(288, size_of::<stream::StreamMetrics>());

    assert_eq!((456, 712).select(), size_of::<CopyBidirectional>());
    assert_eq!((608, 888).select(), size_of::<Entry<CopyBidirectional>>());
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
