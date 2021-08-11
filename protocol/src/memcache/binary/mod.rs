mod meta;

use std::{
    io::{Error, ErrorKind, Result},
    usize,
};

use meta::PacketPos;

pub const HEADER_LEN: usize = 24;

use byteorder::{BigEndian, ByteOrder};

use crate::{MetaType, Protocol, Request, RingSlice};

#[inline]
pub fn body_len(header: &[u8]) -> u32 {
    debug_assert!(header.len() >= 12);
    BigEndian::read_u32(&header[PacketPos::TotalBodyLength as usize..])
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
const OP_CODE_NOOP: u8 = 0x0a;
const OP_CODE_GETK: u8 = 0x0c;
const OP_CODE_GETKQ: u8 = 0x0d;

// 0x09: getq
// 0x0d: getkq
const MULT_GETS: [u8; 128] = [
    0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
];

#[derive(Clone)]
pub struct MemcacheBinary;

impl MemcacheBinary {
    pub fn new() -> Self {
        MemcacheBinary
    }
    // 需要应对gek个各种姿势： getkq...getkq + noop, getkq...getkq + getk，对于quite cmd，肯定是multiget的非结尾请求
    #[inline(always)]
    fn is_multi_get_quite_op(&self, op_code: u8) -> bool {
        MULT_GETS[op_code as usize] == 1
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
            // getMulti的姿势On(quite-cmd) + O1(non-quite-cmd)，最后通常一个noop请求或者getk等 非quite请求 结尾
            if !self.is_multi_get_quite_op(op_code) {
                let pos = read;
                return (true, pos);
            }
        }
        (false, req.len())
    }
    #[inline(always)]
    fn _noreply(&self, req: &[u8]) -> bool {
        req[1] == NOREPLY_MAPPING[req[1] as usize]
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
            let noreply = self._noreply(data);
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
                let mut new = Request::from_vec(v, req.id());
                new.set_noreply();
                new
            }
        }
    }
    #[inline(always)]
    fn parse_request(&self, req: &[u8]) -> Result<(bool, usize)> {
        //debug_assert!(req.len() >= self.min_size());
        if req[PacketPos::Magic as usize] != REQUEST_MAGIC {
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
    // 只要是多个resp cmd（目前是 multiget） 都会调用
    // 1)对于noop结尾，返回noop cmd的长度，在外部trim掉;
    // 2)对于其他non-quite cmd，修改结尾opcode(getk)为getkq;
    #[inline]
    fn trim_tail<T: AsRef<RingSlice>>(&self, _resp: T) -> usize {
        // noop结尾，只需要trim掉一个header长度，否则是getk结尾，需要改为getkq
        let mut read = 0;
        let data = _resp.as_ref();
        let avail = data.len();
        loop {
            // 最后一个cmd是noop，直接trim掉
            if read + HEADER_LEN == avail {
                debug_assert!(
                    !self.is_multi_get_quite_op(data.at(read + PacketPos::Opcode as usize))
                );
                return HEADER_LEN;
            }

            let cmd_len =
                data.read_u32(read + PacketPos::TotalBodyLength as usize) as usize + HEADER_LEN;
            debug_assert!(read + cmd_len <= avail);

            // 如果是最后一个非noop cmd，则肯定是类似getk这样的非quite cmd，把对应opcode改为getkq
            if (read + cmd_len) == avail {
                let opcode_pos = read + PacketPos::Opcode as usize;
                if data.at(opcode_pos) != OP_CODE_GETK {
                    log::warn!("careful - unexpected cmd");
                }
                debug_assert!(!self.is_multi_get_quite_op(data.at(opcode_pos)));
                data.update_byte(opcode_pos, OP_CODE_GETKQ);
                return 0;
            }

            read += cmd_len;
        }
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
    // 会一直持续到非quite response，才算结束，而非仅仅用noop判断，以应对getkq...getkq + getk的场景
    fn parse_response(&self, response: &RingSlice) -> (bool, usize) {
        let mut read = 0;
        let avail = response.available();
        loop {
            if avail < read + HEADER_LEN {
                return (false, avail);
            }
            debug_assert_eq!(response.at(PacketPos::Magic as usize), 0x81);
            let len =
                response.read_u32(read + PacketPos::TotalBodyLength as usize) as usize + HEADER_LEN;
            if self.is_multi_get_quite_op(response.at(read + PacketPos::Opcode as usize)) {
                read += len;
                continue;
            }

            let n = read + len;
            return (avail >= n, n);
        }
    }
    // 轮询response，找出本次查询到的keys，loop所在的位置
    fn keys_response<'a, T>(&self, resp: T) -> Vec<String>
    where
        T: Iterator<Item = &'a RingSlice>,
    {
        let mut keys: Vec<String> = Vec::new();
        for slice in resp {
            self.scan_response_keys(slice, &mut keys);
        }
        return keys;
    }
    // 轮询response，找出本次查询到的keys，loop所在的位置
    fn scan_response_keys(&self, response: &RingSlice, keys: &mut Vec<String>) {
        let mut read = 0;
        let avail = response.available();
        loop {
            if avail < read + HEADER_LEN {
                // 这种情况不应该出现
                log::warn!("found malformed response");

                debug_assert!(false);
                return;
            }
            debug_assert_eq!(response.at(read), 0x81);
            // 如果最后一个是noop请求，说明处理完毕，直接返回;
            if response.at(read + PacketPos::Opcode as usize) == OP_CODE_NOOP {
                return;
            }

            let cmd_len =
                response.read_u32(read + PacketPos::TotalBodyLength as usize) as usize + HEADER_LEN;
            debug_assert!(read + cmd_len <= avail);
            // key 获取
            let key_len = response.read_u16(read + 2);
            let extra_len = response.at(read + 4);
            let key = response.read_bytes(read + HEADER_LEN + extra_len as usize, key_len as usize);
            keys.push(key);

            read += cmd_len;
            if read == avail {
                return;
            }
        }
    }
    // 注意：multiget request 结尾可能是noop，也可能是getk等任何非quite cmd
    fn rebuild_get_multi_request(
        &self,
        current_cmds: &Request,
        found_keys: &Vec<String>,
        new_cmds: &mut Vec<u8>,
    ) {
        let mut read = 0;
        let origin = current_cmds.data();
        let avail = origin.len();
        let mut last_op_pos = 0usize;

        loop {
            // noop 是24 bytes
            debug_assert!(read + HEADER_LEN <= avail);
            debug_assert_eq!(origin[read], 0x80);

            // 如果是noop，直接结束
            if origin[read + PacketPos::Opcode as usize] == OP_CODE_NOOP {
                if new_cmds.len() == 0 {
                    //全部命中，不用构建新的cmd了
                    return;
                }
                new_cmds.extend(&origin[read..read + HEADER_LEN]);
                debug_assert_eq!(read + HEADER_LEN, avail);
                return;
            }

            // 这个cmd可能是最后一个cmd，也可能不是，如果是最后一个，则必须是非quite cmd
            let len = BigEndian::read_u32(&origin[read + 8..]) as usize + HEADER_LEN;
            debug_assert!(read + len <= avail);

            // 找到key，如果是命中的key，则对应cmd被略过
            let key_len = BigEndian::read_u16(&origin[read + PacketPos::Key as usize..]) as usize;
            let extra_len = origin[read + PacketPos::ExtrasLength as usize] as usize;
            let extra_pos = read + HEADER_LEN + extra_len;
            let mut key_data = Vec::new();
            key_data.extend_from_slice(&origin[extra_pos..extra_pos + key_len]);
            let key = String::from_utf8(key_data).unwrap_or_default();
            if !found_keys.contains(&key) {
                // 该key miss，将其cmd写入new_cmds
                last_op_pos = new_cmds.len() + PacketPos::Opcode as usize;
                new_cmds.extend(&origin[read..read + len]);
            }
            read += len;

            // 如果最后一个cmd处理完毕，还没遇到noop，说明请求是getk需要把new cmd的最后一个cmd设为getk
            if read == avail {
                if new_cmds.len() == 0 {
                    return;
                }
                // 对于非noop结尾的getMulit请求，设置最后一个cmd的类型为getk
                new_cmds[last_op_pos] = OP_CODE_GETK;
                return;
            }
        }
    }
}
