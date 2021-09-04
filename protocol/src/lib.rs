//mod controller;
//pub use controller::{Controller, GroupStream};
//

use std::io::Result;

pub mod memcache;

mod operation;
pub use operation::*;

mod request;
pub use request::*;

mod response;
pub use response::*;

use enum_dispatch::enum_dispatch;
use std::collections::HashMap;

use ds::{RingSlice, Slice};
use sharding::Sharding;

// 往client写入时，最大的buffer大小。
pub const MAX_SENT_BUFFER_SIZE: usize = 1024 * 1024;

#[enum_dispatch]
pub trait Protocol: Unpin + Clone + 'static {
    fn parse_request(&self, buf: Slice) -> Result<Option<Request>>;
    // 需要跨分片访问的请求进行分片处理
    // 索引下标是分片id
    fn sharding(&self, req: &Request, sharding: &Sharding) -> HashMap<usize, Request>;
    // req是一个完整的store类型的请求；
    // 当前协议支持noreply
    // 当前req不是noreply
    fn with_noreply(&self, _req: &[u8]) -> Vec<u8> {
        todo!("not supported")
    }
    // 调用方必须确保req包含key，否则可能会panic
    fn meta_type(&self, req: &Request) -> MetaType;
    fn key(&self, req: &Request) -> Slice;
    // 从response中解析出一个完成的response
    fn parse_response(&self, response: &RingSlice) -> Option<Response>;
    // 把resp里面存在的key都去掉，只保留未返回结果的key及对应的命令。
    // 如果所有的key都已返回，则返回None
    fn filter_by_key<'a, R>(&self, req: &Request, resp: R) -> Option<Request>
    where
        R: Iterator<Item = (bool, &'a Response)>;
    fn write_response<'a, R, W>(&self, r: R, w: &mut W)
    where
        W: BackwardWrite,
        R: Iterator<Item = (bool, &'a Response)>;
    // 把一个response，通常是一个get对应的返回，转换为一个Request。
    // 用于将数据从一个实例同步到另外一个实例
    fn convert(&self, _response: &Response, _noreply: bool) -> Option<Request> {
        todo!("convert not supported");
    }
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

#[derive(Debug)]
pub enum MetaType {
    Version,
}

#[derive(Copy, Clone)]
pub enum Resource {
    Memcache,
}

impl Resource {
    #[inline]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Memcache => "mc",
        }
    }
}

pub trait BackwardWrite {
    fn write(&mut self, data: &RingSlice);
    // f: 部分场景需要数据写入完成之后，对数据进行更新
    fn write_on<F: Fn(&mut [u8])>(&mut self, data: &RingSlice, update: F);
}
