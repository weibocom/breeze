mod cacheservice;
mod redisservice;
mod topology;

use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};

use cacheservice::CacheService;
use discovery::TopologyRead;
use protocol::Protocol;
use redisservice::RedisNamespace;
use redisservice::RedisService;
use stream::{AsyncReadAll, AsyncWriteAll, Request, Response};
pub use topology::Topology;

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
