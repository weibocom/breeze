//pub mod buffer;
pub mod handler;
pub mod pipeline;
pub use protocol::callback::*;
pub use protocol::request::*;
mod reconn;

mod context;

pub trait Read {
    fn consume<Out, C: Fn(&[u8]) -> (usize, Out)>(&mut self, c: C) -> Out;
}

mod builder;
pub use builder::*;

pub(crate) mod checker;

mod metric;
pub use metric::StreamMetrics;

mod arena;

mod topology;
pub use topology::CheckedTopology;
