mod meta;
use std::usize;

pub use meta::MemcacheBinaryMetaStream;

pub const HEADER_LEN: usize = 24;

use byteorder::{BigEndian, ByteOrder};

use crate::RingSlice;
use crate::{Protocol, Router};

#[inline]
pub fn body_len(header: &[u8]) -> u32 {
    debug_assert!(header.len() >= 12);
    BigEndian::read_u32(&header[8..])
}
// https://github.com/memcached/memcached/wiki/BinaryProtocolRevamped#command-opcodes
// MC包含Get, Gets, Store, Meta四类命令，索引分别是0-3
const COMMAND_IDX: [u8; 128] = [
    0, 2, 2, 2, 2, 2, 2, 3, 3, 1, 1, 3, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
];
const NO_OP: [u8; 24] = [
    129, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
];
pub struct MemcacheBinary {
    // 上一个完整读取完一个包的位置
    read: usize,
    op_router: MemcacheBinaryOpRoute,
}

impl MemcacheBinary {
    pub fn new() -> Self {
        MemcacheBinary {
            read: 0,
            op_router: MemcacheBinaryOpRoute {},
        }
    }
}

impl Protocol for MemcacheBinary {
    #[inline(always)]
    fn min_size(&self) -> usize {
        HEADER_LEN
    }
    #[inline(always)]
    fn min_last_response_size(&self) -> usize {
        HEADER_LEN
    }
    #[inline]
    fn probe_request_eof(&mut self, req: &[u8]) -> (bool, usize) {
        while self.read as usize + HEADER_LEN <= req.len() {
            // 当前请求的body
            let total = body_len(&req[self.read as usize..]) as usize + HEADER_LEN;
            if self.read as usize + total > req.len() {
                return (false, req.len());
            }
            let op_code = req[self.read + 1];
            self.read += total;
            // 0xd是getq请求，说明当前请求是multiget请求，最后通常一个noop请求结束
            if op_code != 0xd {
                let pos = self.read;
                self.read = 0;
                return (true, pos);
            }
        }
        (false, req.len())
    }
    // 调用方确保req是一个完整的mc的请求包。
    // 第二个字节是op_code。
    #[inline]
    fn op_route(&mut self, req: &[u8]) -> usize {
        self.op_router.route(req)
    }
    // 去掉末尾的NO_OP
    // TODO FIXME
    // 要使用之前必须要流式读取判断
    // req长度要大于24个字节。为了简单，直接使用 noop的code来校验。
    #[inline]
    fn probe_response_eof(&mut self, resp: &[u8]) -> (bool, usize) {
        debug_assert!(resp.len() >= HEADER_LEN);
        let noop = &resp[resp.len() - HEADER_LEN..];
        (noop == NO_OP, resp.len() - HEADER_LEN)
    }
    #[inline]
    fn key<'a>(&mut self, req: &'a [u8]) -> &'a [u8] {
        let extra_len = req[4] as usize;
        let offset = extra_len + HEADER_LEN;
        let key_len = BigEndian::read_u16(&req[2..]) as usize;
        &req[offset..offset + key_len]
    }
    #[inline]
    fn keys<'a>(&mut self, req: &'a [u8]) -> Vec<&'a [u8]> {
        todo!()
    }
    fn build_gets_cmd(&mut self, keys: Vec<&[u8]>) -> Vec<u8> {
        todo!()
    }
    fn probe_response_found(&mut self, response: &[u8]) -> bool {
        debug_assert!(response.len() > HEADER_LEN);
        let status = BigEndian::read_u16(&response[6..]) as usize;
        status == 0
    }
    fn parse_response(&mut self, response: &RingSlice) -> (bool, usize) {
        if response.available() < HEADER_LEN {
            return (false, response.available());
        }
        debug_assert_eq!(response.at(0), 0x81);
        let len = response.read_u32(8) as usize + HEADER_LEN;
        (response.available() >= len, len)
    }

    // 请求命中，status为0，否则部位0
    fn probe_response_succeed_rs(&mut self, response: &RingSlice) -> bool {
        if response.available() < HEADER_LEN {
            return false;
        }
        debug_assert_eq!(response.at(0), 0x81);
        // status 在字节的6、7两个字节
        response.at(6) == 0 && response.at(7) == 0
    }
}

pub struct MemcacheBinaryOpRoute {}
impl MemcacheBinaryOpRoute {
    pub fn new() -> Self {
        MemcacheBinaryOpRoute {}
    }
}
impl Router for MemcacheBinaryOpRoute {
    fn route(&mut self, req: &[u8]) -> usize {
        COMMAND_IDX[req[1] as usize] as usize
    }
}
