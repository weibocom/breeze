//mod controller;
//pub use controller::{Controller, GroupStream};
//

use std::io::Result;

pub mod memcache;
use ds::RingSlice;
use tokio::io::AsyncWrite;

use enum_dispatch::enum_dispatch;

#[enum_dispatch]
pub trait Protocol: Unpin + Clone + 'static {
    // 一个请求包的最小的字节数
    fn min_size(&self) -> usize;
    // 从response读取buffer时，可能读取多次。
    // 限制最后一个包最小返回长度。
    fn min_last_response_size(&self) -> usize;
    // parse会被一直调用，直到返回true.
    // 当前请求是否结束。
    // 一个请求在req中的第多少个字节结束。
    // req包含的是一个完整的请求。
    fn parse_request(&self, req: &[u8]) -> Result<(bool, usize)>;
    // 按照op来进行路由，通常用于读写分离
    fn op_route(&self, req: &[u8]) -> usize;
    // 调用方必须确保req包含key，否则可能会panic
    fn meta_type(&self, req: &[u8]) -> MetaType;
    fn key<'a>(&self, req: &'a [u8]) -> &'a [u8];
    fn keys<'a>(&self, req: &'a [u8]) -> Vec<&'a [u8]>;
    fn build_gets_cmd(&self, keys: Vec<&[u8]>) -> Vec<u8>;
    // 解析当前请求是否结束。返回请求结束位置，如果请求
    // 包含EOF（类似于memcache协议中的END）则返回的位置不包含END信息。
    // 主要用来在进行multiget时，判断请求是否结束。
    fn trim_eof<T: AsRef<RingSlice>>(&self, response: T) -> usize;
    // 从response中解析出一个完成的response
    fn parse_response(&self, response: &RingSlice) -> (bool, usize);
    fn response_found<T: AsRef<RingSlice>>(&self, response: T) -> bool;
}
#[enum_dispatch(Protocol)]
#[derive(Clone)]
pub enum Protocols {
    Mc(memcache::Memcache),
}

impl Protocols {
    pub fn from(name: &str) -> Option<Self> {
        match name {
            "mc" | "memcache" | "memcached" => Some(Self::Mc(memcache::Memcache::new())),
            _ => None,
        }
    }
}

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
        let b = unsafe { std::mem::transmute(buff.unfilled_mut()) };
        let n = self.slice.read(b);
        buff.advance(n);
        self.slice.available() == 0
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
use std::pin::Pin;
use std::task::{Context, Poll};
// 数据读取的时候，要么一次性全部读取，要么都不读取

#[enum_dispatch]
pub trait AsyncReadAll {
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<ResponseItem>>;
    // 处理完poll_next之后的请求调用
    fn poll_done(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>>;
}
impl AsyncWriteAll for tokio::net::TcpStream {}
impl AsyncWriteAll for tokio::net::tcp::OwnedWriteHalf {}

pub enum MetaType {
    Version,
}
