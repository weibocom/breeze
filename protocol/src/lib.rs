#[macro_use]
extern crate lazy_static;

mod flag;
pub use flag::Bit;
pub mod memcache;
pub mod parser;
pub mod phantom;
pub mod redis;
pub use redis::RedisFlager;
pub mod req;
//pub mod resp;
pub mod msgque;
pub mod mysql;

pub use flag::*;
pub use parser::Proto as Protocol;
pub use parser::*;

mod write;
pub use write::*;

pub use req::*;
mod operation;
pub use operation::*;

pub mod callback;
pub mod request;

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

mod error;
pub use error::Error;
pub type Result<T> = std::result::Result<T, Error>;
