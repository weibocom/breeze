mod meta;

use std::io::{Error, ErrorKind, Result};

use crate::MetaStream;
pub use meta::MemcacheBinaryMetaStream;

pub const HEADER_LEN: usize = 24;

use byteorder::{BigEndian, ByteOrder};

use crate::{Protocol, RingSlice};

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

const REQUEST_MAGIC: u8 = 0x80;

#[derive(Clone)]
pub struct MemcacheBinary;

impl MemcacheBinary {
    pub fn new() -> Self {
        MemcacheBinary
    }
    #[inline]
    fn _probe_request(&self, req: &[u8]) -> (bool, usize) {
        let mut read = 0usize;
        while read + HEADER_LEN <= req.len() {
            // 当前请求的body
            let total = body_len(&req[read as usize..]) as usize + HEADER_LEN;
            if read as usize + total > req.len() {
                return (false, req.len());
            }
            let op_code = req[read + 1];
            read += total;
            // 0xd是getq请求，说明当前请求是multiget请求，最后通常一个noop请求结束
            if op_code != 0xd {
                let pos = read;
                return (true, pos);
            }
        }
        (false, req.len())
    }
}

impl Protocol for MemcacheBinary {
    #[inline(always)]
    fn min_size(&self) -> usize {
        HEADER_LEN
    }

    #[inline(always)]
    fn parse_request(&self, req: &[u8]) -> Result<(bool, usize)> {
        debug_assert!(req.len() >= self.min_size());
        if req[0] != REQUEST_MAGIC {
            Err(Error::new(
                ErrorKind::InvalidData,
                "not a valid protocol, the magic number must be 0x80 on mc binary protocol",
            ))
        } else {
            let (done, n) = self._probe_request(req);
            Ok((done, n))
        }
    }

    #[inline(always)]
    fn min_last_response_size(&self) -> usize {
        HEADER_LEN
    }
    // 调用方确保req是一个完整的mc的请求包。
    // 第二个字节是op_code。
    #[inline]
    fn op_route(&self, req: &[u8]) -> usize {
        COMMAND_IDX[req[1] as usize] as usize
    }
    // TODO 只有multiget 都会调用
    #[inline]
    fn trim_eof<T: AsRef<RingSlice>>(&self, resp: T) -> usize {
        HEADER_LEN
    }
    #[inline]
    fn key<'a>(&self, req: &'a [u8]) -> &'a [u8] {
        let extra_len = req[4] as usize;
        let offset = extra_len + HEADER_LEN;
        let key_len = BigEndian::read_u16(&req[2..]) as usize;
        &req[offset..offset + key_len]
    }
    #[inline]
    fn keys<'a>(&self, _req: &'a [u8]) -> Vec<&'a [u8]> {
        todo!()
    }
    fn build_gets_cmd(&self, _keys: Vec<&[u8]>) -> Vec<u8> {
        todo!()
    }
    fn probe_response_found(&self, response: &[u8]) -> bool {
        debug_assert!(response.len() > HEADER_LEN);
        let status = BigEndian::read_u16(&response[6..]) as usize;
        status == 0
    }
    fn parse_response(&self, response: &RingSlice) -> (bool, usize) {
        if response.available() < HEADER_LEN {
            return (false, response.available());
        }
        debug_assert_eq!(response.at(0), 0x81);
        let len = response.read_u32(8) as usize + HEADER_LEN;
        (response.available() >= len, len)
    }

    fn meta(&self, url: &str) -> MetaStream {
        MetaStream::Mc(MemcacheBinaryMetaStream::from(url))
    }
}
