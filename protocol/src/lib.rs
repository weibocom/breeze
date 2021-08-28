//mod controller;
//pub use controller::{Controller, GroupStream};
//

use std::io::Result;

pub mod memcache;
use ds::RingSlice;

mod request;
pub use request::*;

use enum_dispatch::enum_dispatch;

// 往client写入时，最大的buffer大小。
pub const MAX_SENT_BUFFER_SIZE: usize = 1024 * 1024;

#[enum_dispatch]
pub trait Protocol: Unpin + Clone + 'static {
    // parse会被一直调用，直到返回true.
    // 当前请求是否结束。
    // 一个请求在req中的第多少个字节结束。
    // req包含的是一个完整的请求。
    fn parse_request(&self, buf: &[u8]) -> Result<Option<Request>>;
    // req是一个完整的store类型的请求；
    // 当前协议支持noreply
    // 当前req不是noreply
    fn with_noreply(&self, _req: &[u8]) -> Vec<u8> {
        todo!("not supported")
    }
    // 按照op来进行路由，通常用于读写分离
    fn op_route(&self, req: &[u8]) -> usize;
    #[inline(always)]
    fn operation(&self, req: &[u8]) -> Operation {
        self.op_route(req).into()
    }
    // 调用方必须确保req包含key，否则可能会panic
    fn meta_type(&self, req: &[u8]) -> MetaType;
    fn key<'a>(&self, req: &'a [u8]) -> &'a [u8];
    // TODO rebuild_get_multi_request 部分移过来 fishermen
    fn keys<'a>(&self, req: &'a [u8]) -> Vec<&'a [u8]>;
    fn build_gets_cmd(&self, keys: Vec<&[u8]>) -> Vec<u8>;
    // 解析当前请求是否结束。返回请求结束位置，如果请求
    // 包含EOF（类似于memcache协议中的END）则返回的位置不包含END信息。
    // 主要用来在进行multiget时，解析待trim掉的结尾长度，对于noop cmd全部trim，对于非quite的getk，修正opcode
    fn trim_tail<T: AsRef<RingSlice>>(&self, response: T) -> usize;
    // 从response中解析出一个完成的response
    fn parse_response(&self, response: &RingSlice) -> (bool, usize);
    fn response_found<T: AsRef<RingSlice>>(&self, response: T) -> bool;
    // 轮询response，解析出本次查到的keys以及noop所在的位置
    // TODO keys_response ？keys作为返回值 fishermen
    fn scan_response_keys(&self, response: &RingSlice, keys: &mut Vec<String>);
    fn keys_response<'a, T: Iterator<Item = &'a RingSlice>>(&self, response: T) -> Vec<String>;
    // 从当前的cmds中，过滤掉已经查到的keys，然后返回新的请求cmds
    // TODO 新的request 作为返回值 fishermen
    fn rebuild_get_multi_request(
        &self,
        current_cmds: &Request,
        found_keys: &Vec<String>,
        new_req_data: &mut Vec<u8>,
    );
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

#[repr(u8)]
#[derive(Copy, Clone)]
pub enum Operation {
    Get = 0u8,
    Gets,
    Store,
    Meta,
    Other,
}

impl Default for Operation {
    #[inline(always)]
    fn default() -> Self {
        Operation::Other
    }
}

use Operation::*;

impl From<usize> for Operation {
    #[inline(always)]
    fn from(op: usize) -> Self {
        match op {
            0 => Get,
            1 => Gets,
            2 => Store,
            3 => Meta,
            _ => Other,
        }
    }
}
const OP_NAMES: [&'static str; 5] = ["get", "mget", "store", "meta", "other"];
impl Operation {
    #[inline(always)]
    pub fn name(&self) -> &'static str {
        OP_NAMES[*self as u8 as usize]
    }
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
