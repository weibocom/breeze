#[macro_use]
extern crate lazy_static;

mod flag;
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
pub mod metrics;
pub mod msgque;
pub mod uuid;
pub mod vector;

pub use flag::*;
pub use parser::Proto as Protocol;
pub use parser::*;

pub use ds::{Bit, Ext};

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
    Uuid,
    Vector,
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
            Self::Uuid => "uuid",
            Self::Vector => "vector",
        }
    }
}

mod error;
pub use error::Error;
pub type Result<T> = std::result::Result<T, Error>;
