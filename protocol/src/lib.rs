#[macro_use]
extern crate lazy_static;

mod flag;
pub mod memcache;
pub mod redis;

mod operation;
pub use operation::*;

mod request;
pub use request::*;

mod response;

pub use response::*;

pub use flag::*;
pub use parser::Proto as Protocol;
pub use parser::*;
pub use topo::*;

pub use req::*;
mod operation;
pub use operation::*;

pub mod callback;
pub mod request;

pub trait ResponseWriter {
    // 写数据，一次写完
    fn write(&mut self, data: &[u8]) -> Result<()>;
    #[inline(always)]
    fn write_u8(&mut self, v: u8) -> Result<()> {
        self.write(&[v])
    }
}
#[enum_dispatch(Protocol)]
#[derive(Clone)]
pub enum Protocols {
    McBin(memcache::MemcacheBin),
    McText(memcache::MemcacheText),
    Redis(redis::RedisResp2),
}

impl Protocols {
    pub fn try_from(name: &str) -> Result<Self> {
        match name {
            "mc_bin" | "mc" | "memcache" | "memcached" | "rd_bin" | "rd" | "redis" => {
                Ok(Self::McBin(memcache::MemcacheBin::new()))
            }
            "mc_text" | "memcache_text" | "memcached_text" | "redis_text" | "redis_text" => {
                Ok(Self::McText(memcache::MemcacheText::new()))
            }
            "rs" | "redis" => Ok(Self::Redis(redis::RedisResp2::new())),
            _ => Err(Error::new(
                ErrorKind::InvalidData,
                format!("'{}' is not a valid protocol", name),
            )),
        }
        Ok(())
    }
}

#[derive(Copy, Clone)]
pub enum Resource {
    Memcache,
    Redis,
}

impl Resource {
    #[inline]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Memcache => "mc",
            Self::Redis => "redis",
        }
    }
}

use std::str::from_utf8;
use std::time::Duration;
pub trait Builder<P, R, E> {
    fn build(addr: &str, parser: P, rsrc: Resource, service: &str, timeout: Duration) -> E
    where
        E: Endpoint<Item = R>;
}
mod error;
pub use error::Error;
pub type Result<T> = std::result::Result<T, Error>;

impl ResponseWriter for Vec<u8> {
    #[inline(always)]
    fn write(&mut self, data: &[u8]) -> Result<()> {
        ds::vec::Buffer::write(self, data);
        Ok(())
    }
    #[inline(always)]
    fn write_u8(&mut self, v: u8) -> Result<()> {
        self.push(v);
        Ok(())
    }
}
