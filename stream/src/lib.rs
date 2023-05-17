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
pub use builder::BackendBuilder as Builder;
pub use builder::*;

pub(crate) mod checker;
//pub(crate) mod timeout;

//pub(crate) mod gc;
//pub use gc::start_delay_drop;

mod metric;
pub use metric::StreamMetrics;

mod arena;

mod topology;
pub use topology::CheckedTopology;

// 最小2K，至少容纳一个MTU
//pub(crate) const MIN_BUFFER_SIZE: usize = 1024 * 2;
//pub(crate) const MAX_BUFFER_SIZE: usize = 64 << 20;
// 如果连接占用的buff(tx+tx) >= 该值，在pending时就会触发定期回收
//pub(crate) const REFRESH_THREASHOLD: usize = 16 * 1024;
