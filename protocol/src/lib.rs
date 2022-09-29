#[macro_use]
extern crate lazy_static;

mod flag;
pub mod memcache;
pub mod parser;
pub mod phantom;
pub mod redis;
pub mod req;
//pub mod resp;
pub mod msgque;

mod topo;
mod utf8;

pub use flag::*;
pub use parser::Proto as Protocol;
pub use parser::*;
pub use topo::*;

pub use req::*;
mod operation;
pub use operation::*;

pub mod callback;
pub mod request;
pub use utf8::*;

pub trait Writer {
    fn pending(&self) -> usize;
    // 写数据，一次写完
    fn write(&mut self, data: &[u8]) -> Result<()>;
    #[inline]
    fn write_u8(&mut self, v: u8) -> Result<()> {
        self.write(&[v])
    }

    // hint: 提示可能优先写入到cache
    #[inline]
    fn cache(&mut self, _hint: bool) {}

    #[inline]
    fn write_slice(&mut self, data: &ds::RingSlice, oft: usize) -> Result<()> {
        let mut oft = oft;
        let len = data.len();
        log::debug!("+++ will write to client/server/{}:{:?}", oft, data.utf8());
        while oft < len {
            let data = data.read(oft);
            oft += data.len();
            if oft < len {
                // 说明有多次写入，将其cache下来
                self.cache(true);
            }
            self.write(data)?;
        }
        Ok(())
    }
}

#[derive(Copy, Clone)]
pub enum Resource {
    Memcache,
    Redis,
    Phantom,
    MsgQue,
}

impl Resource {
    #[inline]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Memcache => "mc",
            Self::Redis => "redis",
            Self::Phantom => "phantom",
            Self::MsgQue => "msgque",
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

impl Writer for Vec<u8> {
    #[inline]
    fn pending(&self) -> usize {
        self.len()
    }
    #[inline]
    fn write(&mut self, data: &[u8]) -> Result<()> {
        ds::vec::Buffer::write(self, data);
        Ok(())
    }
    #[inline]
    fn write_u8(&mut self, v: u8) -> Result<()> {
        self.push(v);
        Ok(())
    }
}
