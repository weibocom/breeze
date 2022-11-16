#[macro_use]
extern crate lazy_static;

mod flag;
pub mod memcache;
pub mod parser;
pub mod phantom;
pub mod redis;

mod protometrics;
pub mod req;
pub use protometrics::metrics::CbMetrics;
//pub mod resp;
pub mod msgque;

mod topo;

pub use flag::*;
pub use parser::Proto as Protocol;
pub use parser::*;
pub use topo::*;

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
pub trait Metric {
    fn get_metric(&mut self, num: i64) -> i64;
    // fn down_num(&mut self) -> i64;
}

use ds::time::Duration;
pub trait Builder<P, R, E> {
    fn build(addr: &str, parser: P, rsrc: Resource, service: &str, timeout: Duration) -> E
    where
        E: Endpoint<Item = R>;
}

mod error;
pub use error::Error;
pub type Result<T> = std::result::Result<T, Error>;
