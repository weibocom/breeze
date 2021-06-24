//mod controller;
//pub use controller::{Controller, GroupStream};

pub mod chan;
pub mod memcache;
mod slice;
pub use slice::RingSlice;

pub trait ResponseParser {
    fn parse_response(&mut self, response: &RingSlice) -> (bool, usize);
    fn probe_response_found(&mut self, response: &RingSlice) -> bool;
}

use enum_dispatch::enum_dispatch;

#[enum_dispatch]
pub trait Protocol: Unpin {
    // 一个包的最小的字节数
    fn min_size(&self) -> usize;
    // parse会被一直调用，直到返回true.
    // 当前请求是否结束。
    // 一个请求在req中的第多少个字节结束。
    // req包含的是一个完整的请求。
    fn probe_request_eof(&mut self, req: &[u8]) -> (bool, usize);
    // 按照op来进行路由，通常用于读写分离
    fn op_route(&mut self, req: &[u8]) -> usize;
    // 按照key进行路由
    fn key_route(&mut self, req: &[u8], len: usize) -> usize;
    // 调用方必须确保req包含key，否则可能会panic
    fn parse_key<'a>(&mut self, req: &'a [u8]) -> &'a [u8];
    // 解析当前请求是否结束。返回请求结束位置，如果请求
    // 包含EOF（类似于memcache协议中的END）则返回的位置不包含END信息。
    // 主要用来在进行multiget时，判断请求是否结束。
    fn probe_response_eof(&mut self, partial_resp: &[u8]) -> (bool, usize);
    // 解析响应是否命中
    fn probe_response_found(&mut self, response: &[u8]) -> (bool, usize);
}
#[enum_dispatch(Protocol)]
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

impl Clone for Protocols {
    fn clone(&self) -> Self {
        match self {
            Self::Mc(_) => Self::Mc(memcache::Memcache::new()),
        }
    }
}
pub trait Router {
    fn route(&mut self, req: &[u8]) -> usize;
}
