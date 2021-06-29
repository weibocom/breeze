mod get_sync;
mod multi_get;
// mod multi_get_2;
mod operation;
mod pipeline;
//mod request_ref;
mod route;
mod set_sync;
mod sharding;

pub use get_sync::AsyncGetSync;
pub use multi_get::AsyncMultiGet as AsyncMultiGetSharding;
// pub use multi_get_2::AsyncMultiGet;
pub use operation::AsyncOperation;
pub use pipeline::PipeToPingPongChanWrite;
//pub use request_ref::RequestRef;
pub use route::AsyncRoute;
pub use set_sync::AsyncSetSync;
pub use sharding::AsyncSharding;

use tokio::io::AsyncWrite;

/// 该接口是一个marker接口。实现了该接口的AsyncWrite，本身不
/// 会处理buf数据，只会把数据会给chan的接收方，但在数据会给
/// 下游之前，会确保buf是一个完整的request请求。request的格式
/// 由具体的协议决定。方便下由处理。
/// 通常实现要尽可能确保chan处理buf的过程是zero copy的。
/// 输入是pipeline的，输出是ping-pong的。
pub trait AsyncPipeToPingPongChanWrite: AsyncWrite + Unpin {}

/// 标识一个实现了AsyncWrite的接口，写入buf时，只能有以下情况:
/// buf全部写入成功
/// Pending
/// 写入错误
/// 不能出现部分写入成功的情况。方案处理
pub trait AsyncWriteAll {}

/// 确保读取response的时候，类似于NotFound、Stored这样的数据包含
/// 在一个readbuf中，不被拆开，方便判断

impl AsyncWriteAll for tokio::net::TcpStream {}
impl AsyncWriteAll for tokio::net::tcp::OwnedWriteHalf {}

use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};
// 数据读取的时候，要么一次性全部读取，要么都不读取

use enum_dispatch::enum_dispatch;

#[enum_dispatch]
pub trait AsyncReadAll {
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<ResponseItem>>;
    // 处理完poll_next之后的请求调用
    fn poll_done(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>>;
}

use crate::RingSlice;
use tokio::io::ReadBuf;

pub struct ResponseItem {
    slice: RingSlice,
}

impl ResponseItem {
    pub fn from(slice: RingSlice) -> Self {
        Self { slice: slice }
    }
    pub fn from_slice(slice: &'static [u8]) -> Self {
        Self::from(RingSlice::from(
            slice.as_ptr(),
            slice.len().next_power_of_two(),
            0,
            slice.len(),
        ))
    }
    // 返回true，说明所有ResponseItem的数据都写入完成
    pub fn write_to(&mut self, buff: &mut ReadBuf) -> bool {
        self.slice.read(buff)
    }
    pub fn len(&self) -> usize {
        todo!("not supported");
    }
    pub fn append(&mut self, other: ResponseItem) {
        todo!("not supported");
    }
    pub fn advance(&mut self, n: usize) {
        todo!("not supported");
    }
    pub fn backwards(&mut self, n: usize) {
        todo!("not supported");
    }
}

impl AsRef<RingSlice> for ResponseItem {
    fn as_ref(&self) -> &RingSlice {
        &self.slice
    }
}

unsafe impl Send for ResponseItem {}
unsafe impl Sync for ResponseItem {}
