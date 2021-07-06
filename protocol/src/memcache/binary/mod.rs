mod meta;

use std::{
    intrinsics::copy_nonoverlapping,
    io::{Error, ErrorKind, Result},
    usize,
};

pub const HEADER_LEN: usize = 24;

use byteorder::{BigEndian, ByteOrder};
use bytes::BufMut;
use ds::Slice;

use crate::{MetaType, Protocol, RingSlice};

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
const OP_CODE_GETKQ: u8 = 0xd;
const OP_CODE_NOOP: u8 = 0x0a;

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
    //#[inline(always)]
    //fn min_size(&self) -> usize {
    //    HEADER_LEN
    //}

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

            if response.at(read + 1) == OP_CODE_GETKQ {
                read += len;
                continue;
            }

            let n = read + len;
            return (avail >= n, n);
        }
    }
    // 轮询response，找出本次查询到的keys，loop所在的位置
    fn scan_response(&self, response: &RingSlice, keys: &mut Vec<String>) -> (usize) {
        let mut read = 0;
        let avail = response.available();
        loop {
            if avail < read + HEADER_LEN {
                // 这种情况不应该出现
                debug_assert!(false);
                return 0;
            }
            debug_assert!(response.at(read), 0x81);
            // op_getkq 是最后一个response
            if (response.at(read + 1) == OP_CODE_NOOP) {
                return read;
            }
            let len = response.read_u32(read + 8) as usize + HEADER_LEN;
            debug_assert!(read + len <= avail);
            // key 获取
            let key_len = response.read_u16(read + 2);
            let extra_len = response.at(read + 4);
            let key = response.read_bytes(read + HEADER_LEN + extra_len, key_len);
            keys.push(key);

            read += len;
            if read == avail {
                return read;
            }
        }
    }
    fn remove_found_cmd(&self, current_cmds: &Slice, found_keys: Vec<String>) -> Vec<u8> {
        let new_cmds = Vec::new();
        let read = 0;
        let origin = current_cmds.data();
        let avail = origin.len();

        loop {
            // noop 正好是24 bytes
            debug_assert!(read + HEADER_LEN <= avail);
            debug_assert!(origin[read], 0x80);
            if origin[read + 1] == OP_CODE_NOOP {
                new_cmds.put(origin[read..read + HEADER_LEN]);
                return new_cmds;
            }
            let len = BigEndian::read_u32(origin[read + 8]) as usize + HEADER_LEN;
            debug_assert!(read + len <= avail);

            let key_len = BigEndian::read_u16(origin[read + 2]);
            let extra_len = origin[read + 4];
            unsafe {
                let key_bytes = [0u8; key_len];
                copy_nonoverlapping(origin[read + HEADER_LEN + extra_len], key_bytes, key_len);
                let key = String::from_utf8_lossy(key_bytes).to_string();
                if found_keys.contains(key) {
                    // key 已经命中，略过
                    read += len;
                    if read == avail {
                        return new_cmds;
                    }
                    continue;
                }
                new_cmds.put(origin[read..read + len]);
            }
        }
    }
}
