use std::io::{Error, ErrorKind, Result};

use discovery::Inited;
use protocol::Protocol;
use sharding::hash::Hasher;

pub use protocol::Endpoint;

macro_rules! define_topology {
    ($($top:ty, $item:ident, $ep:expr);+) => {

 #[derive(Clone)]
 pub enum Topology<B, E, R, P> {
      $($item($top)),+
 }

 impl<B, E, R, P> Topology<B, E, R, P>  {
     pub fn try_from(parser:P, endpoint:&str) -> Result<Self> {
          match &endpoint[..]{
              $($ep => Ok(Self::$item(parser.into())),)+
              _ => Err(Error::new(ErrorKind::InvalidData, format!("'{}' is not a valid endpoint", endpoint))),
          }
     }
 }
 impl<B, E, R, P> Inited for Topology<B, E, R, P> where E:Inited {
     #[inline]
     fn inited(&self) -> bool {
          match self {
              $(
                  Self::$item(p) => p.inited(),
              )+
          }
     }
 }

impl<B, E, R, P> discovery::TopologyWrite for Topology<B, E, R, P> where P:Sync+Send+Protocol, B:protocol::Builder<P, R, E>, E:Endpoint<Item = R>{
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

impl<B:Send+Sync, E, R, P> protocol::Topology for Topology<B, E, R, P>
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

impl<B, E, R, P> protocol::Endpoint for Topology<B, E, R, P>
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

}


    };
}

use crate::cacheservice::topo::CacheService;
use crate::redisservice::topo::RedisService;

define_topology! {
    RedisService<B, E, R, P>, RedisService, "rs";
    CacheService<B, E, R, P>, CacheService, "cs"
}
