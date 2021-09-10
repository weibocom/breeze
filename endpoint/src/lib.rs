use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};

use discovery::ServiceDiscover;
use protocol::Protocol;

use stream::{AsyncReadAll, AsyncWriteAll, Request, Response};

macro_rules! define_endpoint {
    ($($top:ty, $item:ident, $type_name:tt, $ep:expr);+) => {

       pub enum Topology<P> {
            $($item($top)),+
       }

       impl<P> Topology<P>  {
           pub fn from(parser:P, endpoint:String) -> Option<Self> {
                match &endpoint[..]{
                    $($ep => Some(Self::$item(parser.into())),)+
                    _ => None,
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

       impl<P> discovery::Topology for Topology<P> where P:Sync+Send+Protocol{
           fn update(&mut self, cfg: &str, name: &str) {
               match self {
                    $(Self::$item(s) => discovery::Topology::update(s, cfg, name),)+
               }
           }
       }


        pub enum Endpoint<P> {
            $($item($type_name<P>)),+
        }

        impl<P> Endpoint<P>  {
            pub async fn from_discovery<D>(name: &str, p:P, discovery:D) -> Result<Option<Self>>
                where D: ServiceDiscover<Topology<P>> + Unpin + 'static,
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

use cacheservice::CacheService;
//use pipe::{Pipe, PipeTopology};

define_endpoint! {
//    PipeTopology, Pipe,         Pipe,         "pipe";
    cacheservice::Topology<P>, CacheService, CacheService, "cs"
}
