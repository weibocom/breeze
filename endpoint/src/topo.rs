use std::io::{Error, ErrorKind, Result};

use discovery::Inited;
use ds::time::Duration;
use protocol::{Protocol, Resource};
use sharding::hash::Hasher;

// pub use protocol::Endpoint;

use enum_dispatch::enum_dispatch;

pub trait Endpoint: Sized + Send + Sync {
    type Item;
    fn send(&self, req: Self::Item);
    //#[inline]
    //fn static_send(receiver: usize, req: Self::Item) {
    //    let e = unsafe { &*(receiver as *const Self) };
    //    e.send(req);
    //}
    //
    // 返回hash应该发送的分片idx
    fn shard_idx(&self, _hash: i64) -> usize {
        log::warn!("+++ should not use defatult shard idx");
        panic!("should not use defatult shard idx");
    }
}

impl<T, R> Endpoint for &T
where
    T: Endpoint<Item = R>,
{
    type Item = R;
    #[inline]
    fn send(&self, req: R) {
        (*self).send(req)
    }

    #[inline]
    fn shard_idx(&self, hash: i64) -> usize {
        (*self).shard_idx(hash)
    }
}

impl<T, R> Endpoint for std::sync::Arc<T>
where
    T: Endpoint<Item = R>,
{
    type Item = R;
    #[inline]
    fn send(&self, req: R) {
        (**self).send(req)
    }
    #[inline]
    fn shard_idx(&self, hash: i64) -> usize {
        (**self).shard_idx(hash)
    }
}

#[enum_dispatch]
pub trait Topology: Endpoint {
    #[inline]
    fn exp_sec(&self) -> u32 {
        86400
    }
    fn hasher(&self) -> &Hasher;
}

impl<T> Topology for std::sync::Arc<T>
where
    T: Topology,
{
    #[inline]
    fn exp_sec(&self) -> u32 {
        (**self).exp_sec()
    }
    #[inline]
    fn hasher(&self) -> &Hasher {
        (**self).hasher()
    }
}
pub trait TopologyCheck: Sized {
    fn check(&mut self) -> Option<Self>;
}

pub trait Single {
    fn single(&self) -> bool;
    fn disable_single(&self);
    fn enable_single(&self);
}

impl<T> Single for std::sync::Arc<T>
where
    T: Single,
{
    #[inline]
    fn single(&self) -> bool {
        (**self).single()
    }
    #[inline]
    fn disable_single(&self) {
        (**self).disable_single()
    }
    #[inline]
    fn enable_single(&self) {
        (**self).enable_single()
    }
}

pub trait Builder<P, R, E> {
    fn build(addr: &str, parser: P, rsrc: Resource, service: &str, timeout: Duration) -> E
    where
        E: Endpoint<Item = R>;
}

macro_rules! define_topology {
    ($($top:ty, $item:ident, $ep:expr);+) => {

 #[derive(Clone)]
 pub enum TopologyProtocol<B, E, R, P> {
      $($item($top)),+
 }

 impl<B, E, R, P> TopologyProtocol<B, E, R, P>  {
     pub fn try_from(parser:P, endpoint:&str) -> Result<Self> {
          match &endpoint[..]{
              $($ep => Ok(Self::$item(parser.into())),)+
              _ => Err(Error::new(ErrorKind::InvalidData, format!("'{}' is not a valid endpoint", endpoint))),
          }
     }
 }
 impl<B, E, R, P> Inited for TopologyProtocol<B, E, R, P> where E:Inited {
     #[inline]
     fn inited(&self) -> bool {
          match self {
              $(
                  Self::$item(p) => p.inited(),
              )+
          }
     }
 }

impl<B, E, R, P> discovery::TopologyWrite for TopologyProtocol<B, E, R, P> where P:Sync+Send+Protocol, B:Builder<P, R, E>, E:Endpoint<Item = R>+Single{
    #[inline]
    fn update(&mut self, name: &str, cfg: &str) {
        match self {
             $(Self::$item(s) => discovery::TopologyWrite::update(s, name, cfg),)+
        }
    }
    #[inline]
    fn disgroup<'a>(&self, path: &'a str, cfg: &'a str) -> Vec<(&'a str, &'a str)> {
        match self {
             $(Self::$item(s) => discovery::TopologyWrite::disgroup(s, path, cfg),)+
        }
    }
    #[inline]
    fn need_load(&self) -> bool {
        match self {
             $(Self::$item(s) => discovery::TopologyWrite::need_load(s),)+
        }
    }
    #[inline]
    fn load(&mut self) {
        match self {
             $(Self::$item(s) => discovery::TopologyWrite::load(s),)+
        }
    }
}

impl<B:Send+Sync, E, R, P> Topology for TopologyProtocol<B, E, R, P>
where P:Sync+Send+Protocol, E:Endpoint<Item = R>, R:protocol::Request{
    #[inline]
    fn hasher(&self) -> &Hasher {
        match self {
            $(
                Self::$item(p) => p.hasher(),
            )+
        }
    }
    #[inline]
    fn exp_sec(&self) -> u32 {
        match self {
            $(
                Self::$item(p) => p.exp_sec(),
            )+
        }
    }
}

impl<B, E, R, P> Endpoint for TopologyProtocol<B, E, R, P>
where P:Sync+Send+Protocol, E:Endpoint<Item = R>,
    R: protocol::Request,
    P: Protocol,
    B:Send+Sync,
{
    type Item = R;
    #[inline]
    fn send(&self, req:Self::Item) {
        match self {
            $(
                Self::$item(p) => p.send(req),
            )+
        }
    }

    #[inline]
    fn shard_idx(&self, hash: i64) -> usize {
        match self {
            $(
                Self::$item(p) => p.shard_idx(hash),
            )+
        }
    }
}


    };
}

use crate::cacheservice::topo::CacheService;
use crate::msgque::topo::MsgQue;
use crate::phantomservice::topo::PhantomService;
use crate::redisservice::topo::RedisService;

define_topology! {
    RedisService<B, E, R, P>, RedisService, "rs";
    CacheService<B, E, R, P>, CacheService, "cs";
    PhantomService<B, E, R, P>, PhantomService, "pt";
    MsgQue<B, E, R, P>, MsgQue, "mq"
}
