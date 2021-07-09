mod meta;

use std::{
    intrinsics::copy_nonoverlapping,
    io::{Error, ErrorKind, Result},
    usize,
};

pub const HEADER_LEN: usize = 24;

use byteorder::{BigEndian, ByteOrder};
use futures::io::ReadVectored;

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

const REQUEST_MAGIC: u8 = 0x80;
const OP_CODE_GETKQ: u8 = 0xd;
const OP_CODE_GETQ: u8 = 0x09;
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
            if op_code != OP_CODE_GETKQ {
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
            } else if response.at(read + 1) == OP_CODE_GETQ {
                println!("==== check who send getq ===");
                read += len;
                continue;
            }

            let n = read + len;
            return (avail >= n, n);
        }
    }
    // 轮询response，找出本次查询到的keys，loop所在的位置
    fn scan_response_keys<T: AsRef<RingSlice>>(&self, resp_wrapper: T, keys: &mut Vec<String>) {
        let mut read = 0;
        let response = resp_wrapper.as_ref();
        let avail = response.available();
        loop {
            if avail < read + HEADER_LEN {
                // 这种情况不应该出现
                debug_assert!(false);
                return;
            }
            debug_assert_eq!(response.at(read), 0x81);
            // op_getkq 是最后一个response
            if response.at(read + 1) == OP_CODE_NOOP {
                return;
            }
            let len = response.read_u32(read + 8) as usize + HEADER_LEN;
            debug_assert!(read + len <= avail);
            // key 获取
            let key_len = response.read_u16(read + 2);
            let extra_len = response.at(read + 4);
            let key = response.read_bytes(read + HEADER_LEN + extra_len as usize, key_len as usize);
            keys.push(key);

            read += len;
            if read == avail {
                return;
            }
        }
    }
    fn rebuild_get_multi_request(
        &self,
        current_cmds: &Request,
        found_keys: &Vec<String>,
        new_cmds: &mut Vec<u8>,
    ) {
        let mut read = 0;
        let origin = current_cmds.data();
        let avail = origin.len();

        let mut i = 0;
        println!("{} - 0-{:?}", i, current_cmds.data());
        loop {
            i += 1;
            // noop 是24 bytes
            debug_assert!(read + HEADER_LEN <= avail);
            debug_assert_eq!(origin[read], 0x80);
            log::debug!("{} - 1-{:?}", i, new_cmds);

            // get-multi的结尾是noop
            if origin[read + 1] == OP_CODE_NOOP {
                new_cmds.extend(&origin[read..read + HEADER_LEN]);
                debug_assert_eq!(read + HEADER_LEN, avail);
                println!("{} - 3-{:?}", i, new_cmds);
                return;
            }

            // 非noop cmd一定不是最后一个cmd
            let len = BigEndian::read_u32(&origin[read + 8..]) as usize + HEADER_LEN;
            debug_assert!(read + len < avail);

            // 找到key，如果是命中的key，则对应cmd被略过
            let key_len = BigEndian::read_u16(&origin[read + 2..]) as usize;
            let extra_len = origin[read + 4] as usize;
            let extra_pos = read + HEADER_LEN + extra_len;
            let mut key_data = Vec::new();
            key_data.extend_from_slice(&origin[extra_pos..extra_pos + key_len]);
            let key = String::from_utf8(key_data).unwrap_or_default();
            println!("{} - cmd key: {:?}, found_keys: {:?}", i, key, found_keys);
            if found_keys.contains(&key) {
                // key 已经命中，略过
                read += len;
                continue;
            }

            // 该key miss，将其cmd写入new_cmds
            new_cmds.extend(&origin[read..read + len]);
            read += len;
            println!("{} - 2-{:?}", i, new_cmds);
        }
    }

    // 消息结尾标志的长度，对不同协议、不同请求不同
    fn tail_size_for_multi_get(&self) -> usize {
        return HEADER_LEN;
    }
}
