mod meta;

use std::io::{Error, ErrorKind, Result};

pub const HEADER_LEN: usize = 24;

use byteorder::{BigEndian, ByteOrder};

use crate::{MetaType, Protocol, Request, RingSlice};

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
// OP_CODE对应的noreply code。
const NOREPLY_MAPPING: [u8; 128] = [
    0x09, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x09, 0x0a, 0x0b, 0x0d, 0x0d, 0x19, 0x1a,
    0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
    0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f,
    0x30, 0x32, 0x32, 0x34, 0x34, 0x36, 0x36, 0x38, 0x38, 0x3a, 0x3a, 0x3c, 0x3c, 0x3d, 0x3e, 0x3f,
    0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0,
];

const REQUEST_MAGIC: u8 = 0x80;
const OP_CODE_GETQ: u8 = 0xd;

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
    #[inline]
    // 如果当前请求已经是noreply了，则直接clone。
    // 否则把数据复制出来，再更改op_code
    fn copy_noreply(&self, req: &Request) -> Request {
        let data = req.data();
        debug_assert!(data.len() >= HEADER_LEN);
        if req.noreply() {
            req.clone()
        } else {
            let noreply = data[1] == NOREPLY_MAPPING[data[1] as usize];
            if noreply {
                let mut new = req.clone();
                new.set_noreply();
                new
            } else {
                let mut v = vec![0u8; data.len()];
                use std::ptr::copy_nonoverlapping as copy;
                unsafe {
                    copy(data.as_ptr(), v.as_mut_ptr(), data.len());
                }
                v[1] = NOREPLY_MAPPING[data[1] as usize];
                let mut new = Request::from_vec(v, req.id().clone());
                new.set_noreply();
                new
            }
        }
    }
    #[inline(always)]
    fn parse_request(&self, req: &[u8]) -> Result<(bool, usize)> {
        //debug_assert!(req.len() >= self.min_size());
        if req[0] != REQUEST_MAGIC {
            Err(Error::new(
                ErrorKind::InvalidData,
                "not a valid protocol, the magic number must be 0x80 on mc binary protocol",
            ))
        } else {
            if req.len() < HEADER_LEN {
                return Ok((false, req.len()));
            }
            let (done, n) = self._probe_request(req);
            Ok((done, n))
        }
    }

    // 调用方确保req是一个完整的mc的请求包。
    // 第二个字节是op_code。
    #[inline]
    fn op_route(&self, req: &[u8]) -> usize {
        COMMAND_IDX[req[1] as usize] as usize
    }
    fn meta_type(&self, _req: &[u8]) -> MetaType {
        MetaType::Version
    }
    // TODO 只有multiget 都会调用
    #[inline]
    fn trim_eof<T: AsRef<RingSlice>>(&self, _resp: T) -> usize {
        HEADER_LEN
    }
    #[inline]
    fn key<'a>(&self, req: &'a [u8]) -> &'a [u8] {
        debug_assert!(req.len() >= HEADER_LEN);
        let extra_len = req[4] as usize;
        let offset = extra_len + HEADER_LEN;
        let key_len = BigEndian::read_u16(&req[2..]) as usize;
        debug_assert!(key_len + offset <= req.len());
        &req[offset..offset + key_len]
    }
    #[inline]
    fn keys<'a>(&self, _req: &'a [u8]) -> Vec<&'a [u8]> {
        todo!()
    }
    fn build_gets_cmd(&self, _keys: Vec<&[u8]>) -> Vec<u8> {
        todo!()
    }
    fn response_found<T: AsRef<RingSlice>>(&self, response: T) -> bool {
        let slice = response.as_ref();
        debug_assert!(slice.len() >= HEADER_LEN);
        slice.at(6) == 0 && slice.at(7) == 0
    }
    fn parse_response(&self, response: &RingSlice) -> (bool, usize) {
        let mut read = 0;
        let avail = response.available();
        loop {
            if avail < read + HEADER_LEN {
                return (false, avail);
            }
            debug_assert_eq!(response.at(0), 0x81);
            let len = response.read_u32(read + 8) as usize + HEADER_LEN;

            if response.at(read + 1) == OP_CODE_GETQ {
                read += len;
                continue;
            }

            let n = read + len;
            return (avail >= n, n);
        }
    }
}
