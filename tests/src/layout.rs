// 不要轻易变更这里面的测试用例，除非你知道你在做什么。拉相关同学进行方案评审。
use std::{
    mem::size_of,
    sync::{atomic::AtomicU32, Arc},
};

use protocol::Parser;
use stream::{Backend, Request};
type Endpoint = Arc<Backend<Request>>;
//type Topology = endpoint::Topology<Builder, Endpoint, Request, Parser>;

type Stream = rt::Stream<tokio::net::TcpStream>;

type Handler<'r> = stream::handler::Handler<'r, Request, Parser, Stream>;

type Builder = stream::Builder<Parser, Request>;
type CacheService = endpoint::cacheservice::topo::CacheService<Builder, Endpoint, Request, Parser>;
type RedisService = endpoint::redisservice::topo::RedisService<Builder, Endpoint, Request, Parser>;
type PhantomService =
    endpoint::phantomservice::topo::PhantomService<Builder, Endpoint, Request, Parser>;
type MsgQue = endpoint::msgque::topo::MsgQue<Builder, Endpoint, Request, Parser>;

#[test]
fn check_layout() {
    assert_eq!(32, size_of::<ds::RingSlice>());
    assert_eq!(8, size_of::<protocol::Context>());
    assert_eq!(216, size_of::<protocol::callback::CallbackContext>());
    assert_eq!(
        size_of::<protocol::Context>(),
        size_of::<protocol::redis::RequestContext>()
    );
    assert_eq!(24, size_of::<protocol::Flag>());
    assert_eq!(1, size_of::<protocol::Resource>());
    let guard_size = if cfg!(debug_assertions) { 224 } else { 184 };
    assert_eq!(guard_size, size_of::<stream::buffer::StreamGuard>());
    assert_eq!(56, size_of::<ds::queue::PinnedQueue<AtomicU32>>());
    assert_eq!(16, size_of::<metrics::Metric>());
    assert_eq!(64, size_of::<metrics::Item>());
    assert_eq!(1, size_of::<Parser>());
    assert_eq!(48, size_of::<Backend<Request>>());
    let stream_size = if cfg!(debug_assertions) { 248 } else { 200 };
    assert_eq!(stream_size, size_of::<Stream>());
    let handler_size = if cfg!(debug_assertions) { 552 } else { 464 };
    assert_eq!(handler_size, size_of::<Handler<'static>>());
    assert_eq!(0, size_of::<Builder>());

    //assert_eq!(400, size_of::<Topology>());
    assert_eq!(96, size_of::<CacheService>());
    assert_eq!(264, size_of::<RedisService>());
    assert_eq!(192, size_of::<PhantomService>());
    assert_eq!(392, size_of::<MsgQue>());
}
