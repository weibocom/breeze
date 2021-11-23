use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};

use discovery::{Inited, TopologyRead};
use protocol::Protocol;

use stream::{AsyncReadAll, AsyncWriteAll, Request, Response};

macro_rules! define_endpoint {
    ($($top:ty, $item:ident, $type_name:tt, $ep:expr);+) => {

       #[derive(Clone)]
       pub enum Topology<P> {
            $($item($top)),+

       }

       impl<P> Topology<P>  {
           pub fn try_from(parser:P, endpoint:String) -> Result<Self> {
                match &endpoint[..]{
                    $($ep => Ok(Self::$item(parser.into())),)+
                    _ => Err(Error::new(ErrorKind::InvalidData, format!("'{}' is not a valid endpoint", endpoint))),
                }
           }
       }
       impl<P> Inited for Topology<P> {
           fn inited(&self) -> bool {
                match self {
                    $(
                        Self::$item(p) => p.inited(),
                    )+
                }
           }
       }

$(
    // 支持Topology enum自动转换成具体的类型
    impl<P> std::ops::Deref for Topology<P> {
        type Target = $top;
        fn deref(&self) -> &Self::Target {
            match self {
                Self::$item(t) => t,
                // 如果有多个实现，把该注释去掉
                //_ => panic!("topology {} not matched", stringify!($top)),
            }
        }
    }
)+

       impl<P> discovery::TopologyWrite for Topology<P> where P:Sync+Send+Protocol{
           fn update(&mut self, name: &str, cfg: &str) {
               match self {
                    $(Self::$item(s) => discovery::TopologyWrite::update(s, name, cfg),)+
               }
           }
           fn gc(&mut self) {
               match self {
                    $(Self::$item(s) => discovery::TopologyWrite::gc(s),)+
               }
           }
       }

        pub enum Endpoint<P> {
            $($item($type_name<P>)),+
        }

        impl<P> Endpoint<P>  {
            pub async fn from_discovery<D>(name: &str, p:P, discovery:D) -> Result<Option<Self>>
                where D: TopologyRead<Topology<P>> + Unpin + 'static,
                P:protocol::Protocol,
            {
                match name {
                    $($ep => Ok(Some(Self::$item($type_name::from_discovery(p, discovery).await?))),)+
                    _ => Ok(None),
                }
            }
        }

        impl<P> AsyncReadAll for Endpoint<P> where P: Unpin+Protocol{
            #[inline]
            fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Response>> {
                match &mut *self {
                    $(Self::$item(ref mut p) => Pin::new(p).poll_next(cx),)+
                }
            }
        }

        impl<P> AsyncWriteAll for Endpoint<P> where P:Unpin+Protocol{
            #[inline]
            fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &Request) -> Poll<Result<()>>{
                match &mut *self {
                    $(Self::$item(ref mut p) => Pin::new(p).poll_write(cx, buf),)+
                }
            }
        }
    };
}

mod cacheservice;
//mod pipe;
//mod redisservice;
use cacheservice::CacheService;
//use redisservice::RedisService;
//use pipe::{Pipe, PipeTopology};

define_endpoint! {
//    PipeTopology, Pipe,         Pipe,         "pipe";
    // cacheservice::Topology<P>, CacheService, CacheService, "mc";
    // redisservice::Topology<P>, RedisService, RedisService, "rs"
    cacheservice::Topology<P>, CacheService, CacheService, "rs"
}
