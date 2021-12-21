pub mod memcache;
pub mod parser;
pub mod redis;
pub mod req;
pub mod resp;
mod topo;

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
}
