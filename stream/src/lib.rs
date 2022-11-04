pub mod buffer;
pub mod handler;
pub mod pipeline;
mod shards;
pub use protocol::callback::*;
pub use protocol::request::*;
pub use shards::*;
mod reconn;

pub trait Read {
    fn consume<Out, C: Fn(&[u8]) -> (usize, Out)>(&mut self, c: C) -> Out;
}

mod builder;
pub use builder::BackendBuilder as Builder;
pub use builder::*;

pub(crate) mod checker;
//pub(crate) mod timeout;

//pub(crate) mod gc;
//pub use gc::start_delay_drop;

mod metric;
pub use metric::CbMetrics as StreamMetrics;

pub(crate) const MIN_BUFFER_SIZE: usize = 1024;
pub(crate) const MAX_BUFFER_SIZE: usize = 64 << 20;
