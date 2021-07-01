use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};

use discovery::ServiceDiscover;
use protocol::Protocol;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

macro_rules! define_endpoint {
    ($($top:tt, $item:ident, $type_name:tt, $ep:expr);+) => {

       #[derive(Clone)]
       pub enum Topology<P> {
            $($item($top<P>)),+
       }

       impl<P> Topology<P>  {
           pub fn from(parser:P, endpoint:String) -> Option<Self> {
                match &endpoint[..]{
                    $($ep => Some(Self::$item(parser.into())),)+
                    _ => None,
                }
           }
       }

       impl<P> discovery::Topology for Topology<P> where P:Clone+Sync+Send+Protocol+'static{
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

        impl<P> AsyncRead for Endpoint<P> where P: Unpin+Protocol{
            fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context, buf: &mut ReadBuf) -> Poll<Result<()>> {
                match &mut *self {
                    $(Self::$item(ref mut p) => Pin::new(p).poll_read(cx, buf),)+
                }
            }
        }

        impl<P> AsyncWrite for Endpoint<P> where P:Unpin+Protocol{
            fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<Result<usize>>{
                match &mut *self {
                    $(Self::$item(ref mut p) => Pin::new(p).poll_write(cx, buf),)+
                }
            }
            fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
                match &mut *self {
                    $(Self::$item(ref mut p) => Pin::new(p).poll_flush(cx),)+
                }
            }
            fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
                match &mut *self {
                    $(Self::$item(ref mut p) => Pin::new(p).poll_shutdown(cx),)+
                }
            }
        }
    };
}

mod cacheservice;
mod pipe;

use cacheservice::CacheService;
use cacheservice::Topology as CSTopology;
use pipe::{Pipe, PipeTopology};

define_endpoint! {
    PipeTopology, Pipe,         Pipe,         "pipe";
    CSTopology, CacheService, CacheService, "cs"
}

impl<P> left_right::Absorb<(String, String)> for Topology<P>
where
    P: Clone + Sync + Send + Protocol + 'static,
{
    fn absorb_first(&mut self, cfg: &mut (String, String), _other: &Self) {
        discovery::Topology::update(self, &cfg.0, &cfg.1)
    }
    fn sync_with(&mut self, first: &Self) {
        *self = first.clone();
    }
}
