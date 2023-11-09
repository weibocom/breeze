use std::io::{Error, ErrorKind, Result};

use discovery::Inited;
use protocol::Protocol;
use sharding::hash::{Hash, HashKey};

use crate::msgque::topo::MsgQue;

use enum_dispatch::enum_dispatch;

pub trait Endpoint: Sized + Send + Sync {
    type Item;
    fn send(&self, req: Self::Item);
    // 返回hash应该发送的分片idx
    fn shard_idx(&self, _hash: i64) -> usize {
        log::warn!("+++ should not use defatult shard idx");
        panic!("should not use defatult shard idx");
    }
}

use std::ops::Deref;
impl<T, R, E> Endpoint for E
where
    E: Deref<Target = T> + Send + Sync,
    T: Endpoint<Item = R>,
{
    type Item = R;
    #[inline(always)]
    fn send(&self, req: R) {
        (**self).send(req)
    }

    #[inline(always)]
    fn shard_idx(&self, hash: i64) -> usize {
        (**self).shard_idx(hash)
    }
}

#[enum_dispatch]
pub trait Topology: Endpoint + Hash {
    #[inline]
    fn exp_sec(&self) -> u32 {
        86400
    }
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

// pub trait Builder<P, R, E> {
//     fn build(addr: &str, parser: P, rsrc: Resource, service: &str, timeout: Timeout) -> E {
//         Self::auth_option_build(addr, parser, rsrc, service, timeout, Default::default())
//     }

//     // TODO: ResOption -> AuthOption
//     fn auth_option_build(
//         addr: &str,
//         parser: P,
//         rsrc: Resource,
//         service: &str,
//         timeout: Timeout,
//         option: ResOption,
//     ) -> E;
// }

macro_rules! define_topology {
    ($($top:ty, $item:ident, $ep:expr);+) => {

 #[derive(Clone)]
 pub enum TopologyProtocol<R, P> {
      $($item($top)),+
 }

 impl<R, P> TopologyProtocol<R, P>  {
     pub fn try_from(parser:P, endpoint:&str) -> Result<Self> {
          match &endpoint[..]{
              $($ep => Ok(Self::$item(parser.into())),)+
              _ => Err(Error::new(ErrorKind::InvalidData, format!("'{}' is not a valid endpoint", endpoint))),
          }
     }
 }
 impl<R, P> Inited for TopologyProtocol<R, P> {
     #[inline]
     fn inited(&self) -> bool {
          match self {
              $(
                  Self::$item(p) => p.inited(),
              )+
          }
     }
 }

impl<R, P> discovery::TopologyWrite for TopologyProtocol<R, P> where P:Sync+Send+Protocol, R:protocol::Request{
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

impl<R, P> Hash for TopologyProtocol<R, P>
where P:Sync+Send+Protocol, R:protocol::Request{
    #[inline]
    fn hash<K:HashKey>(&self, k:&K) -> i64 {
        match self {
            $(
                Self::$item(p) => p.hash(k),
            )+
        }
    }
}

impl<R, P> Topology for TopologyProtocol<R, P>
where P:Sync+Send+Protocol, R:protocol::Request{
    #[inline]
    fn exp_sec(&self) -> u32 {
        match self {
            $(
                Self::$item(p) => p.exp_sec(),
            )+
        }
    }
}

impl<R, P> Endpoint for TopologyProtocol<R, P>
where P:Sync+Send+Protocol,
    R: protocol::Request,
    P: Protocol,

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

use crate::kv::topo::KvService;
#[cfg(feature = "mq")]
use crate::msgque::topo::MsgQue;

use crate::cacheservice::topo::CacheService;
use crate::phantomservice::topo::PhantomService;
use crate::redisservice::topo::RedisService;
use crate::uuid::topo::UuidService;

define_topology! {
    MsgQue<R, P>, MsgQue, "mq";
    RedisService<R, P>, RedisService, "rs";
    CacheService<R, P>, CacheService, "cs";
    PhantomService<R, P>, PhantomService, "pt";
    KvService<R, P>, KvService, "kv";
    UuidService<R, P>, UuidService, "uuid"
}

// 从环境变量获取是否开启后端资源访问的性能模式
#[inline]
fn is_performance_tuning_from_env() -> bool {
    context::get().timeslice()
}

pub(crate) trait PerformanceTuning {
    fn tuning_mode(&self) -> bool;
}

impl PerformanceTuning for String {
    fn tuning_mode(&self) -> bool {
        is_performance_tuning_from_env()
            || match self.as_str() {
                "distance" | "timeslice" => true,
                _ => false,
            }
    }
}

impl PerformanceTuning for bool {
    fn tuning_mode(&self) -> bool {
        is_performance_tuning_from_env() || *self
    }
}
