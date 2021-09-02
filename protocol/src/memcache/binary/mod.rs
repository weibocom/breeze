use std::collections::HashMap;
use std::io::{Error, ErrorKind, Result};

use ds::{RingSlice, Slice};
use sharding::Sharding;

use crate::{MetaType, Operation, Protocol, Request, Response};

mod meta;
use meta::*;

#[derive(Clone)]
pub struct MemcacheBinary;
impl Protocol for MemcacheBinary {
    // 当前请求必须不是noreply的
    #[inline]
    fn with_noreply(&self, req: &[u8]) -> Vec<u8> {
        let mut v = vec![0u8; req.len()];
        use std::ptr::copy_nonoverlapping as copy;
        unsafe {
            copy(req.as_ptr(), v.as_mut_ptr(), req.len());
        }
        // 更新op code标识位为noreply
        v[PacketPos::Opcode as usize] = NOREPLY_MAPPING[req[PacketPos::Opcode as usize] as usize];
        v
    }
    #[inline(always)]
    fn parse_request(&self, req: Slice) -> Result<Option<Request>> {
        if !req.request() {
            Err(Error::new(
                ErrorKind::InvalidData,
                "the magic number must be 0x80",
            ))
        } else {
            Ok(parse_request(&req))
        }
    }
    #[inline]
    fn sharding(&self, req: &Request, shard: &Sharding) -> HashMap<usize, Request> {
        // 只有multiget才有分片
        debug_assert_eq!(req.operation(), Operation::Gets);
        unsafe {
            let klen = req.keys().len();
            let mut keys = Vec::with_capacity(klen);
            for cmd in req.keys() {
                let key = cmd.key();
                if key.len() > 0 {
                    keys.push(key);
                }
            }
            debug_assert!(keys.len() > 0);
            let last_cmd = req.keys().get_unchecked(klen - 1);

            let sharded = shard.shardings(keys);
            if sharded.len() == 1 {
                let mut ret = HashMap::with_capacity(1);
                let (s_idx, _) = sharded.iter().next().expect("only one shard");
                ret.insert(*s_idx, req.clone());
                return ret;
            }
            let mut sharded_req = HashMap::with_capacity(sharded.len());
            for (s_idx, indice) in sharded.iter() {
                debug_assert!(indice.len() > 0);
                let mut cmd: Vec<u8> = Vec::with_capacity(req.len());
                let mut keys: Vec<Slice> = Vec::with_capacity(indice.len() + 1);
                for idx in indice.iter() {
                    debug_assert!(*idx < klen);
                    let cmd_i = req.keys().get_unchecked(*idx);
                    keys.push(Slice::new(
                        cmd.as_ptr().offset(cmd.len() as isize) as usize,
                        cmd_i.len(),
                    ));
                    cmd_i.copy_to_vec(&mut cmd);
                }
                // 最后一个是noop请求，则需要补充上noop请求
                if last_cmd.noop() {
                    last_cmd.copy_to_vec(&mut cmd);
                } else {
                    // 最后一个请求不是noop. 把最后一个请求的opcode与原始的req保持一致
                    let last = keys.get_unchecked(keys.len() - 1);
                    let last_op_idx = cmd.len() - last.len() + PacketPos::Opcode as usize;
                    cmd[last_op_idx] = last_cmd.op();
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
    #[inline(always)]
    fn key(&self, req: &Request) -> Slice {
        debug_assert_eq!(req.keys().len(), 1);
        req.key()
    }
    // 会一直持续到非quite response，才算结束，而非仅仅用noop判断，以应对getkq...getkq + getk的场景
    #[inline]
    fn parse_response(&self, response: &RingSlice) -> Option<Response> {
        parse_response(response)
    }

    fn filter_by_key<'a, R>(&self, req: &Request, mut resp: R) -> Option<Request>
    where
        R: Iterator<Item = (bool, &'a Response)>,
    {
        debug_assert!(req.operation() == Operation::Get || req.operation() == Operation::Gets);
        debug_assert!(req.keys().len() > 0);
        if self.is_single_get(req) {
            if let Some((_, response)) = resp.next() {
                if response.status_ok() && !response.noop() {
                    return None;
                }
            }
            return Some(req.clone());
        }
        // 有多个key
        let found_keys = self.keys_response(resp, req.keys().len());
        let mut cmd = Vec::with_capacity(req.len());
        let mut keys_slice = Vec::with_capacity(req.keys().len());
        // 遍历所有的请求key，如果response中没有，则写入到command中
        for cmd_i in req.keys() {
            let key = cmd_i.key();
            if key.len() > 0 {
                // 未找到，则写入新的request
                if !found_keys.contains_key(&key.clone().into()) {
                    // 写入的是原始的命令, 不是key
                    cmd_i.copy_to_vec(&mut cmd);
                    keys_slice.push(cmd_i.clone());
                }
            } else {
                // 只有最后一个noop会不存在key
                // 判断之前是否有请求，避免cmd里面只有一个空的noop请求
                debug_assert!(cmd_i.noop());
                if cmd.len() > 0 {
                    cmd_i.copy_to_vec(&mut cmd);
                    keys_slice.push(cmd_i.clone());
                }
            }
        }
        if keys_slice.len() > 0 {
            // 设置最后一个key的op_code, 与request的最后一个key的opcode保持一致即可
            let last_len = unsafe { keys_slice.get_unchecked(keys_slice.len() - 1).len() };
            let last_op_pos = cmd.len() - last_len + PacketPos::Opcode as usize;
            cmd[last_op_pos] = self.last_key_op(req);
            let new = Request::from_request(cmd, keys_slice, req);

            debug_assert_eq!(new.keys().len() + found_keys.len(), req.keys().len());
            Some(new)
        } else {
            None
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
                match last.op() {
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
    #[inline]
    fn is_single_get(&self, req: &Request) -> bool {
        let keys = req.keys();
        match keys.len() {
            0 | 1 => true,
            // 最后一个请求是NOOP
            2 => unsafe { keys.get_unchecked(keys.len() - 1).noop() },
            _ => false,
        }
    }
    // 最后一个key的op_code
    #[inline]
    fn last_key_op(&self, req: &Request) -> u8 {
        let keys = req.keys();
        debug_assert!(keys.len() > 0);
        unsafe { keys.get_unchecked(keys.len() - 1).op() }
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
            for cmd in one_respone.keys() {
                if cmd.status_ok() {
                    let key = cmd.key();
                    if key.len() > 0 {
                        keys.insert(key, ());
                    }
                }
            }
        }
        debug_assert!(keys.len() <= exptects);
        return keys;
    }
}
