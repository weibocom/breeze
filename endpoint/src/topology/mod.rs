mod inner;
mod layer;
mod seq;

use std::{
    collections::HashMap,
    io::{Error, ErrorKind, Result},
};

use discovery::Inited;
use protocol::{Protocol, Resource};
use stream::LayerRole;

pub use inner::Inner;
pub use layer::Layer;
pub use seq::Seq;

use crate::{cacheservice::MemcacheTopology, redisservice::RedisTopology};

#[derive(Clone)]
pub enum Topology<P> {
    RedisTopo(RedisTopology<P>),
    MemcacheTopo(MemcacheTopology<P>),
}

impl<P> Topology<P>
where
    P: Protocol,
{
    pub fn try_from(parser: P, endpoint: String) -> Result<Self> {
        match &endpoint[..] {
            "rs" => Ok(Self::RedisTopo(parser.into())),
            "cs" => Ok(Self::MemcacheTopo(parser.into())),
            _ => Err(Error::new(
                ErrorKind::InvalidData,
                format!("'{}' is not a valid endpoint", endpoint),
            )),
        }
    }
}

impl<P> Inited for Topology<P> {
    fn inited(&self) -> bool {
        match self {
            Self::RedisTopo(r) => r.inited(),
            Self::MemcacheTopo(c) => c.inited(),
        }
    }
}

impl<P> discovery::TopologyWrite for Topology<P>
where
    P: Sync + Send + Protocol,
{
    fn resource(&self) -> protocol::Resource {
        match self {
            Self::RedisTopo(_) => Resource::Redis,
            Self::MemcacheTopo(_) => Resource::Memcache,
        }
    }
    fn update(&mut self, name: &str, cfg: &str, hosts: &HashMap<String, Vec<String>>) {
        match self {
            Self::RedisTopo(r) => discovery::TopologyWrite::update(r, name, cfg, hosts),
            Self::MemcacheTopo(c) => discovery::TopologyWrite::update(c, name, cfg, hosts),
        }
    }

    fn gc(&mut self) {
        match self {
            Self::RedisTopo(r) => discovery::TopologyWrite::gc(r),
            Self::MemcacheTopo(c) => discovery::TopologyWrite::gc(c),
        }
    }
}

pub(crate) trait VisitAddress {
    fn visit<F: FnMut(&str)>(&self, f: F);
    fn select<F: FnMut(LayerRole, usize, &str)>(&self, f: F);
}

impl VisitAddress for Vec<String> {
    fn visit<F: FnMut(&str)>(&self, mut f: F) {
        for addr in self.iter() {
            f(addr)
        }
    }
    // 每一层可能有多个pool，所以usize表示pool编号，新增LayerRole表示层次
    fn select<F: FnMut(LayerRole, usize, &str)>(&self, mut f: F) {
        for (_i, addr) in self.iter().enumerate() {
            f(LayerRole::Unknow, 0, addr);
        }
    }
}
impl VisitAddress for Vec<(LayerRole, Vec<String>)> {
    fn visit<F: FnMut(&str)>(&self, mut f: F) {
        for (_role, layers) in self.iter() {
            for addr in layers.iter() {
                f(addr)
            }
        }
    }
    fn select<F: FnMut(LayerRole, usize, &str)>(&self, mut f: F) {
        // for (i, layers) in self.iter().enumerate() {
        for (i, (role, layers)) in self.iter().enumerate() {
            for addr in layers.iter() {
                f(role.clone(), i, addr);
            }
        }
    }
}
