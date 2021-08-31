mod meta;
use meta::*;

use std::collections::HashMap;
use std::io::{Error, ErrorKind, Result};

use meta::PacketPos;

use byteorder::{BigEndian, ByteOrder};

use crate::{MetaType, Operation, Protocol, Request, Response};

use ds::{RingSlice, Slice};
use sharding::Sharding;

#[derive(Clone)]
pub struct MemcacheBinary;
impl Protocol for MemcacheBinary {
    // 当前请求必须不是noreply的
    #[inline]
    fn with_noreply(&self, req: &[u8]) -> Vec<u8> {
        debug_assert_eq!(self._noreply(req), false);
        let mut v = vec![0u8; req.len()];
        use std::ptr::copy_nonoverlapping as copy;
        unsafe {
            copy(req.as_ptr(), v.as_mut_ptr(), req.len());
        }
        v[1] = NOREPLY_MAPPING[req[1] as usize];
        v
    }
    #[inline(always)]
    fn parse_request(&self, req: &[u8]) -> Result<Option<Request>> {
        if req[PacketPos::Magic as usize] != REQUEST_MAGIC {
            Err(Error::new(
                ErrorKind::InvalidData,
                "not a valid protocol, the magic number must be 0x80 on mc binary protocol",
            ))
        } else {
            if req.len() < HEADER_LEN {
                return Ok(None);
            }
            Ok(self._probe_request(req))
        }
    }
    #[inline]
    fn sharding(&self, req: &Request, shard: &Sharding) -> HashMap<usize, Request> {
        // 只有multiget才有分片
        debug_assert_eq!(req.operation(), Operation::Gets);
        unsafe {
            let klen = req.keys().len();
            let mut keys = Vec::with_capacity(klen);
            for i in 0..klen - 1 {
                let key = self.key(req.keys().get_unchecked(i));
                debug_assert!(key.len() > 0);
                keys.push(key);
            }
            let last_key = req.keys().get_unchecked(klen - 1);
            let noop = last_key[PacketPos::Opcode as usize] == OP_CODE_NOOP;
            if !noop {
                keys.push(self.key(last_key));
            }

            let sharded = shard.shardings(keys);
            if sharded.len() == 1 {
                let mut ret = HashMap::with_capacity(1);
                let (s_idx, _) = sharded.iter().next().expect("only one shard");
                ret.insert(*s_idx, req.clone());
                return ret;
            }
            let mut sharded_req = HashMap::with_capacity(sharded.len());
            for (s_idx, indice) in sharded.iter() {
                let mut cmd: Vec<u8> = Vec::with_capacity(req.len());
                let mut keys: Vec<Slice> = Vec::with_capacity(indice.len() + 1);
                for idx in indice.iter() {
                    let cmd_i = req.keys().get_unchecked(*idx);
                    keys.push(Slice::new(cmd.as_ptr() as usize, cmd_i.len()));
                    cmd_i.copy_to_vec(&mut cmd);
                }
                // 处理end
                // 如果最后一个是noop请求，则需要补充上noop请求
                // 否则，把最后一个请求的opcode与原始的req保持一致
                if noop {
                    last_key.copy_to_vec(&mut cmd);
                } else {
                    let last = keys.get_unchecked(keys.len() - 1);
                    let last_op_idx = cmd.len() - last.len() + PacketPos::Opcode as usize;
                    cmd[last_op_idx] = last_key.at(PacketPos::Opcode as usize);
                }
                let new = Request::from_request(cmd, keys, req);
                sharded_req.insert(*s_idx, new);
            }
            sharded_req
        }
    }

    fn meta_type(&self, _req: &Request) -> MetaType {
        MetaType::Version
    }
    // 只要是多个resp cmd（目前是 multiget） 都会调用
    // 1)对于noop结尾，返回noop cmd的长度，在外部trim掉;
    // 2)对于其他non-quite cmd，修改结尾opcode(getk)为getkq;
    #[inline]
    fn key<'a>(&self, req: &'a [u8]) -> &'a [u8] {
        debug_assert!(req.len() >= HEADER_LEN);
        let extra_len = req[PacketPos::ExtrasLength as usize] as usize;
        let offset = extra_len + HEADER_LEN;
        let key_len = BigEndian::read_u16(&req[PacketPos::Key as usize..]) as usize;
        debug_assert!(key_len + offset <= req.len());
        &req[offset..offset + key_len]
    }
    // 会一直持续到非quite response，才算结束，而非仅仅用noop判断，以应对getkq...getkq + getk的场景
    #[inline]
    fn parse_response(&self, response: &RingSlice) -> Option<Response> {
        let mut read = 0;
        let avail = response.len();
        let mut keys = Vec::with_capacity(8);
        loop {
            if avail < read + HEADER_LEN {
                return None;
            }
            debug_assert_eq!(response.at(read + PacketPos::Magic as usize), 0x81);
            let len =
                response.read_u32(read + PacketPos::TotalBodyLength as usize) as usize + HEADER_LEN;
            if avail < read + len {
                return None;
            }
            keys.push(response.sub_slice(read, len));
            if self.is_multi_get_quite_op(response.at(read + PacketPos::Opcode as usize)) {
                read += len;
                continue;
            }
            read += len;

            return Some(Response::from(response.sub_slice(0, read), keys));
        }
    }

    fn filter_by_key<'a, R>(&self, req: &Request, mut resp: R) -> Option<Request>
    where
        R: Iterator<Item = (bool, &'a Response)>,
    {
        debug_assert!(req.operation() == Operation::Get || req.operation() == Operation::Gets);
        debug_assert!(req.keys().len() > 0);
        if self.is_single_get(req) {
            if let Some((_, response)) = resp.next() {
                if self.status_ok(&response) {
                    return None;
                }
            }
            Some(req.clone())
        } else {
            // 有多个key
            let found_keys = self.keys_response(resp, req.keys().len());
            let mut cmd = Vec::with_capacity(req.len());
            let mut keys_slice = Vec::with_capacity(req.keys().len());
            // 遍历所有的请求key，如果response中没有，则写入到command中
            for req_key in req.keys() {
                let key = self.key(req_key);
                let key_slice = Slice::from(key);
                if key.len() > 0 {
                    // 未找到，则写入新的request
                    if !found_keys.contains_key(&key_slice.clone().into()) {
                        // 写入的是原始的命令, 不是key
                        req_key.copy_to_vec(&mut cmd);
                        keys_slice.push(req_key.clone());
                    }
                } else {
                    // 只有最后一个noop会不存在key
                    req_key.copy_to_vec(&mut cmd);
                    keys_slice.push(req_key.clone());
                }
            }
            if keys_slice.len() > 0 {
                // 设置最后一个key的op_code
                let last_len = unsafe { keys_slice.get_unchecked(keys_slice.len() - 1).len() };
                let last_op_pos = cmd.len() - last_len + PacketPos::Opcode as usize;
                cmd[last_op_pos] = self.last_key_op(req);
                let new = Request::from_request(cmd, keys_slice, req);
                Some(new)
            } else {
                None
            }
        }
    }
    // 需要特殊处理multiget请求。
    // multiget请求有两种方式，一种是 getkq + noop; 另外一种是 getkq + getk.
    // 如果是noop请求，需要将前n-1个response中的noop给truncate掉。
    // 如果是getkq + getk。则需要将前n-1个请求最后一个getk请求，变更为getkq
    #[inline]
    fn write_response<'a, R, W>(&self, r: R, w: &mut W)
    where
        W: crate::BackwardWrite,
        R: Iterator<Item = (bool, &'a Response)>,
    {
        for (last, response) in r {
            // 最后一个请求，不需要做变更，直接写入即可。
            if last {
                w.write(response, 0);
                break;
            }
            // 不是最后一个请求，则处理response的最后一个key
            let kl = response.keys().len();
            if kl > 0 {
                let last = unsafe { response.keys().get_unchecked(kl - 1) };
                match last.at(PacketPos::Opcode as usize) {
                    OP_CODE_NOOP => w.write(response, HEADER_LEN),
                    OP_CODE_GETK => w.write_on(response, |data| {
                        debug_assert_eq!(response.len(), data.len());
                        data[PacketPos::Opcode as usize] = OP_CODE_GETKQ;
                    }),
                    _ => w.write(response, 0),
                }
            }
        }
    }
}

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
    fn _probe_request(&self, req: &[u8]) -> Option<Request> {
        let op = self.op_route(req).into();
        let mut read = 0usize;
        // 包含的是整个请求，不仅仅是key
        let mut keys: Vec<Slice> = Vec::with_capacity(req.len() >> 32);
        while read + HEADER_LEN <= req.len() {
            // 当前请求的body
            let total = body_len(&req[read as usize..]) as usize + HEADER_LEN;
            if read as usize + total > req.len() {
                return None;
            }
            keys.push(Slice::from(&req[read..read + total]));
            let op_code = req[read + 1];
            read += total;
            // getMulti的姿势On(quite-cmd) + O1(non-quite-cmd)，最后通常一个noop请求或者getk等 非quite请求 结尾
            if !self.is_multi_get_quite_op(op_code) {
                let pos = read;
                return Some(Request::new(&req[0..pos], op, keys));
            }
        }
        None
    }
    #[inline(always)]
    fn _noreply(&self, req: &[u8]) -> bool {
        req[1] == NOREPLY_MAPPING[req[1] as usize]
    }
    // 调用方确保req是一个完整的mc的请求包。
    // 第二个字节是op_code。
    #[inline]
    fn op_route(&self, req: &[u8]) -> usize {
        COMMAND_IDX[req[1] as usize] as usize
    }
    #[inline]
    fn status_ok(&self, response: &RingSlice) -> bool {
        debug_assert!(response.len() >= HEADER_LEN);
        response.at(6) == 0 && response.at(7) == 0
    }
    #[inline]
    fn key_response(&self, r: &RingSlice) -> RingSlice {
        debug_assert!(r.len() >= HEADER_LEN);
        let extra_len = r.at(PacketPos::ExtrasLength as usize) as usize;
        let offset = extra_len + HEADER_LEN;
        let key_len = r.read_u16(PacketPos::Key as usize) as usize;
        debug_assert!(key_len + offset <= r.len());
        r.sub_slice(offset, key_len)
    }
    // 是否只包含了一个key。只有get请求才会用到
    #[inline]
    fn is_single_get(&self, req: &Request) -> bool {
        let keys = req.keys();
        match keys.len() {
            0 | 1 => true,
            // 最后一个请求是NOOP
            2 => unsafe {
                keys.get_unchecked(keys.len() - 1)
                    .at(PacketPos::Opcode as usize)
                    == OP_CODE_NOOP
            },
            _ => false,
        }
    }
    // 最后一个key的op_code
    #[inline]
    fn last_key_op(&self, req: &Request) -> u8 {
        let keys = req.keys();
        debug_assert!(keys.len() > 0);
        let last = unsafe { keys.get_unchecked(keys.len() - 1) };
        last.at(PacketPos::Opcode as usize)
    }
    // 轮询response，找出本次查询到的keys，loop所在的位置
    #[inline(always)]
    fn keys_response<'a, T>(&self, resp: T, exptects: usize) -> HashMap<RingSlice, ()>
    where
        T: Iterator<Item = (bool, &'a Response)>,
    {
        let mut keys = HashMap::with_capacity(exptects * 3 / 2);
        // 解析response中的key
        for (_last, one_respone) in resp {
            for response_key in one_respone.keys() {
                // response里面存储的key是一个完整的协议请求
                if self.status_ok(response_key) {
                    keys.insert(self.key_response(response_key), ());
                }
            }
        }
        return keys;
    }
}
