mod cacheservice;
mod redisservice;
mod seq;
mod topology;

use std::collections::HashMap;
use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};

use cacheservice::CacheService;
use cacheservice::MemcacheTopology;
use discovery::{Inited, TopologyRead};
use protocol::{Protocol, Resource};
use redisservice::RedisNamespace;
use redisservice::RedisService;
use redisservice::RedisTopology;
use stream::{AsyncReadAll, AsyncWriteAll, Request, Response};

#[derive(Clone)]
pub enum Topology<P> {
    RedisService(RedisTopology<P>),
    CacheService(MemcacheTopology<P>),
}

impl<P> Topology<P>
where
    P: Protocol,
{
    pub fn try_from(parser: P, endpoint: String) -> Result<Self> {
        match &endpoint[..] {
            "rs" => Ok(Self::RedisService(parser.into())),
            "cs" => Ok(Self::CacheService(parser.into())),
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
            Self::RedisService(r) => r.inited(),
            Self::CacheService(c) => c.inited(),
        }
    }
}

impl<P> discovery::TopologyWrite for Topology<P>
where
    P: Sync + Send + Protocol,
{
    fn resource(&self) -> protocol::Resource {
        match self {
            Self::RedisService(_) => Resource::Redis,
            Self::CacheService(_) => Resource::Memcache,
        }
    }
    fn update(&mut self, name: &str, cfg: &str, hosts: &HashMap<String, Vec<String>>) {
        match self {
            Self::RedisService(r) => discovery::TopologyWrite::update(r, name, cfg, hosts),
            Self::CacheService(c) => discovery::TopologyWrite::update(c, name, cfg, hosts),
        }
    }

    fn gc(&mut self) {
        match self {
            Self::RedisService(r) => discovery::TopologyWrite::gc(r),
            Self::CacheService(c) => discovery::TopologyWrite::gc(c),
        }
    }
}

pub enum Endpoint<P> {
    RedisService { redis_service: RedisService<P> },
    CacheService { cache_service: CacheService<P> },
}

impl<P> Endpoint<P> {
    pub async fn from_discovery<D>(name: &str, p: P, discovery: D) -> Result<Option<Self>>
    where
        D: TopologyRead<Topology<P>> + Unpin + 'static,
        P: protocol::Protocol,
    {
        match name {
            "rs" => Ok(Some(Self::RedisService {
                redis_service: RedisService::from_discovery(p, discovery).await?,
            })),
            "cs" => Ok(Some(Self::CacheService {
                cache_service: CacheService::from_discovery(p, discovery).await?,
            })),
            _ => Ok(None),
        }
    }
}

impl<P> AsyncReadAll for Endpoint<P>
where
    P: Unpin + Protocol,
{
    #[inline]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Response>> {
        match &mut *self {
            Self::RedisService {
                ref mut redis_service,
            } => Pin::new(redis_service).poll_next(cx),
            Self::CacheService {
                ref mut cache_service,
            } => Pin::new(cache_service).poll_next(cx),
            //Endpoint::RedisService {ref mut p} => Pin::new(p).poll_next(cx),
        }
    }
}

impl<P> AsyncWriteAll for Endpoint<P>
where
    P: Unpin + Protocol,
{
    #[inline]
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &Request) -> Poll<Result<()>> {
        match &mut *self {
            Self::RedisService {
                ref mut redis_service,
            } => Pin::new(redis_service).poll_write(cx, buf),
            Self::CacheService {
                ref mut cache_service,
            } => Pin::new(cache_service).poll_write(cx, buf),
            //Endpoint::RedisService {ref mut p} => Pin::new(p).poll_next(cx),
        }
    }
}
