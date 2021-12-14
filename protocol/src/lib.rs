use std::io::{Error, ErrorKind, Result};

pub mod memcache;
pub mod redis;

mod operation;
pub use operation::*;

mod request;
pub use request::*;

mod response;

pub use response::*;

use enum_dispatch::enum_dispatch;

use ds::{RingSlice, Slice};
use sharding::Sharding;

// 往client写入时，最大的buffer大小。
pub const MAX_SENT_BUFFER_SIZE: usize = 1024 * 1024;

#[enum_dispatch]
pub trait Protocol: Unpin + Clone + 'static + Send + Sync {
    fn resource(&self) -> Resource;
    fn parse_request(&self, buf: Slice) -> Result<Option<Request>>;
    // 需要跨分片访问的请求进行分片处理
    // 索引下标是分片id
    fn sharding(&self, req: &Request, sharding: &Sharding) -> Vec<(usize, Request)>;
    // req是一个完整的store类型的请求；
    // 当前协议支持noreply
    // 当前req不是noreply
    fn with_noreply(&self, _req: &[u8]) -> Vec<u8> {
        todo!("not supported")
    }
    // 调用方必须确保req包含key，否则可能会panic
    fn meta_type(&self, req: &Request) -> MetaType;
    fn key(&self, req: &Request) -> Slice;
    fn req_gets(&self, req: &Request) -> bool;
    fn req_cas_or_add(&self, req: &Request) -> bool;
    // 从response中解析出一个完成的response
    fn parse_response(&self, response: &RingSlice) -> Option<Response>;
    // 将response转换为回种数据的cmd,并将数据写入request_buff中，返回待会写的cmds的Slice结构
    fn convert_to_writeback_request(
        &self,
        request: &Request,
        response: &Response,
        expire_seconds: u32,
    ) -> Result<Vec<Request>>;
    // 对gets指令做修正，以满足标准协议
    fn convert_gets(&self, request: &Request);
    // 把resp里面存在的key都去掉，只保留未返回结果的key及对应的命令。
    // 如果所有的key都已返回，则返回None
    fn filter_by_key<'a, R>(&self, req: &Request, resp: R) -> Option<Request>
    where
        R: Iterator<Item = &'a Response>;
    fn write_response<'a, R, W>(&self, r: R, w: &mut W)
    where
        W: BackwardWrite,
        R: Iterator<Item = &'a Response>;
    // 把一个response，通常是一个get对应的返回，转换为一个Request。
    // 用于将数据从一个实例同步到另外一个实例
    fn convert(&self, _response: &Response, _noreply: bool) -> Option<Request> {
        todo!("convert not supported");
    }
}
#[enum_dispatch(Protocol)]
#[derive(Clone)]
pub enum Protocols {
    McBin(memcache::MemcacheBin),
    McText(memcache::MemcacheText),
    Redis(redis::RedisResp2),
}

impl Protocols {
    pub fn try_from(name: &str) -> Result<Self> {
        match name {
            "mc_bin" | "mc" | "memcache" | "memcached" | "rd_bin" | "rd" | "redis" => {
                Ok(Self::McBin(memcache::MemcacheBin::new()))
            }
            "mc_text" | "memcache_text" | "memcached_text" | "redis_text" | "redis_text" => {
                Ok(Self::McText(memcache::MemcacheText::new()))
            }
            "rs" | "redis" => Ok(Self::Redis(redis::RedisResp2::new())),
            _ => Err(Error::new(
                ErrorKind::InvalidData,
                format!("'{}' is not a valid protocol", name),
            )),
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
    Redis,
}

impl Resource {
    #[inline]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Memcache => "mc",
            Self::Redis => "redis",
        }
    }
}

pub trait BackwardWrite {
    fn write(&mut self, data: &RingSlice);
}
