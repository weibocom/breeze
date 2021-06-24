use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};

use discovery::ServiceDiscover;
use protocol::Protocol;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

macro_rules! define_endpoint {
    ($($top:tt, $item:ident, $type_name:tt, $ep:expr);+) => {

       #[derive(Clone)]
       pub enum Topology {
            Empty,
            $($item($top)),+
       }

       impl Default for Topology {
           fn default() -> Self {
               Topology::Empty
           }
       }
       impl From<String> for Topology {
           fn from(endpoint:String) -> Self {
                match &endpoint[..]{
                    $($ep => Self::$item($top::default()),)+
                    _ => {println!("{} not supported endpoint name", endpoint); Self::Empty},
                }
           }
       }

       impl discovery::Topology for Topology {
           fn update(&mut self, cfg: &str, name: &str) {
               match self {
                    $(Self::$item(s) => s.update(cfg, name),)+
                   Self::Empty => {
                        println!("empty topology request received");
                   }
               }
           }
       }


        pub enum Endpoint<P> {
            $($item($type_name<P>)),+
        }

        impl<P> Endpoint<P>  {
            pub async fn from_discovery<D>(name: &str, p:P, discovery:D) -> Result<Self>
                where D: ServiceDiscover<Topology> + Unpin + 'static,
                P:protocol::Protocol+Clone,
            {
                match name {
                    $($ep => Ok(Self::$item($type_name::from_discovery(p, discovery).await?)),)+
                    _ => panic!("not supported endpoint name"),
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

impl left_right::Absorb<(String, String)> for Topology {
    fn absorb_first(&mut self, cfg: &mut (String, String), _other: &Self) {
        discovery::Topology::update(self, &cfg.0, &cfg.1)
    }
    fn sync_with(&mut self, first: &Self) {
        *self = first.clone();
    }
}
