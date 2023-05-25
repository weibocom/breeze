#[macro_use]
extern crate lazy_static;

mod flag;
pub use flag::Bit;
pub mod memcache;
pub mod parser;
// pub mod phantom;
pub mod redis;
pub use redis::RedisFlager;
//for test
pub use redis::packet::Packet;
pub mod req;
//pub mod resp;
pub mod kv;
pub mod msgque;

pub use flag::*;
pub use parser::Proto as Protocol;
pub use parser::*;

//mod write;
//pub use write::*;
mod stream;
pub use stream::*;

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
    Mysql,
}

impl Resource {
    #[inline]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Memcache => "mc",
            Self::Redis => "redis",
            Self::Phantom => "phantom",
            Self::MsgQue => "msgque",
            Self::Mysql => "mysql",
        }
    }
}

mod error;
pub use error::Error;
pub type Result<T> = std::result::Result<T, Error>;
