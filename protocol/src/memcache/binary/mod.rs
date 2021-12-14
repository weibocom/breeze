use std::collections::HashMap;
use std::io::{Error, ErrorKind, Result};
use std::vec;

use ds::{RingSlice, Slice};
use sharding::Sharding;

use crate::{MetaType, Operation, Protocol, Request, Response};

mod meta;
use meta::*;

mod packet;
use packet::*;

use ds::Buffer;

#[derive(Clone)]
pub struct MemcacheBinary;
impl Protocol for MemcacheBinary {
    fn resource(&self) -> crate::Resource {
        crate::Resource::Memcache
    }
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

        // cas 置零
        let mut idx: usize = 0;
        while idx < CAS_LEN {
            v[PacketPos::Cas as usize + idx] = 0;
            idx += 1;
        }
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
    fn sharding(&self, req: &Request, shard: &Sharding) -> Vec<(usize, Request)> {
        // 只有multiget才有分片
        // debug_assert_eq!(req.operation(), Operation::MGet);
        unsafe {
            let klen = req.keys().len();
            let mut keys = Vec::with_capacity(klen);
            for cmd in req.keys() {
                debug_assert!(cmd.key().len() > 0);
                keys.push(cmd.key());
            }
            debug_assert!(keys.len() > 0);

            let sharded = shard.shardings(keys);
            let last_cmd = req.last_key();
            let noop = last_cmd.quite_get(); // 如果最后一个请求是quite，说明当前请求是noop请求。
            let mut sharded_req = Vec::with_capacity(sharded.len());
            for (s_idx, indice) in sharded.iter().enumerate() {
                if indice.len() > 0 {
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
                    if noop {
                        req.take_noop().copy_to_vec(&mut cmd);
                    } else {
                        // 最后一个请求不是noop. 把最后一个请求的opcode与原始的req保持一致
                        let last = keys.get_unchecked(keys.len() - 1);
                        let last_op_idx = cmd.len() - last.len() + PacketPos::Opcode as usize;
                        cmd[last_op_idx] = last_cmd.op();
                    }
                    let new = Request::from_request(cmd, keys, req);
                    sharded_req.push((s_idx, new));
                }
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
    // 对于二进制协议，get都会返回unique id，相当于get直接是文本协议的gets
    // 多层访问要以master为基准，需要扩展一个新的gets opcode
    fn req_gets(&self, req: &Request) -> bool {
        if req.keys().len() > 0 {
            return req.keys()[0].op() == OP_CODE_GETS || req.keys()[0].op() == OP_CODE_GETSQ;
        }
        return false;
    }

    // 多层访问，set 带了unique id就是cas；
    // 多层访问中，cas、add等写操作，只支持非pipeline操作
    fn req_cas_or_add(&self, req: &Request) -> bool {
        debug_assert!(req.keys().len() == 1);
        let opcode = req.op();
        if opcode == OP_CODE_ADD {
            return true;
        } else if opcode == OP_CODE_SET {
            // 带cas的set是cas操作
            if req.cas() != 0 {
                return true;
            }
        }
        return false;
    }

    // 会一直持续到非quite response，才算结束，而非仅仅用noop判断，以应对getkq...getkq + getk的场景
    #[inline]
    fn parse_response(&self, response: &RingSlice) -> Option<Response> {
        parse_response(response)
    }

    // 一个response中可能包含1个或多个key-response响应，转换时需要注意，返回每个cmd的长度
    // TODO: 对于get请求，需要根据请求获取key；对于gets请求，当前只支持请求response中带key的请求模式；
    fn convert_to_writeback_request(
        &self,
        request: &Request,
        response: &Response,
        expire_seconds: u32,
    ) -> Result<Vec<Request>> {
        // 如果response中没有keys，说明不是get/gets，或者没有查到数据
        if response.keys().len() == 0 {
            log::info!("ignore for response has no results");
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "not keys found in response",
            ));
        }

        let mut requests_wb = Vec::with_capacity(response.keys().len());
        // 轮询response的cmds，构建回写request
        for rsp_cmd in response.keys() {
            // 只为status为ok的resp构建回种req
            if !rsp_cmd.status_ok() {
                continue;
            }

            // check response中是否有key，没有则使用request中的key，同时计算request的长度
            let mut key_len = rsp_cmd.key_len();
            let use_request_key = key_len == 0 && request.keys().len() == 1;
            let mut req_cmd_len = rsp_cmd.len() + 4; // 4 为expire flag的长度
            if use_request_key {
                // 这里后面需要通过opaque来匹配key，目前暂时只支持getkq+getk/noop 方式
                debug_assert!(request.keys().len() == 1);
                debug_assert!(rsp_cmd.opaque() == request.keys()[0].opaque());
                key_len = request.keys()[0].key_len();
                req_cmd_len += key_len as usize;
            }

            // 先用rsp的精确长度预分配，避免频繁分配内存
            let mut req_cmd: Vec<u8> = Vec::with_capacity(req_cmd_len);

            /*============= 构建request header =============*/
            req_cmd.push(Magic::Request as u8); // magic: [0]
            req_cmd.push(Opcode::SetQ as u8); // opcode: [1]
            req_cmd.write_u16(key_len); // key len: [2,3]
            let extra_len = rsp_cmd.extra_len() + 4 as u8; // get response中的extra 应该是4字节，作为set的 flag，另外4字节是set的expire
            debug_assert!(extra_len == 8);
            req_cmd.push(extra_len); // extra len: [4]
            req_cmd.push(0 as u8); // data type，全部为0: [5]
            req_cmd.write_u16(0 as u16); // vbucket id, 回写全部为0,pos [6,7]
            let total_body_len = extra_len as u32 + key_len as u32 + rsp_cmd.value_len();
            req_cmd.write_u32(total_body_len); // total body len [8-11]
            req_cmd.write_u32(0 as u32); // opaque: [12, 15]
            req_cmd.write_u64(0 as u64); // cas: [16, 23]

            /*============= 构建request body =============*/
            req_cmd.write(rsp_cmd.extra_or_flag().data()); // extra之flag: [24, 27]
            req_cmd.write_u32(expire_seconds); // extra之expiration：[28,31]
            if use_request_key {
                req_cmd.write(request.keys()[0].key().data());
            } else {
                req_cmd.write(rsp_cmd.key().data());
            }
            req_cmd.write(rsp_cmd.value().data());

            if req_cmd.capacity() > req_cmd_len {
                log::info!(
                    "req writeback init capacity should bigger:{}/{}",
                    req_cmd_len,
                    req_cmd.capacity()
                );
            }
            let cmds = vec![Slice::from(&req_cmd[0..req_cmd.len()])];
            let req =
                Request::from_data(req_cmd, cmds, request.id().clone(), true, Operation::Store);

            requests_wb.push(req);
        }
        return Ok(requests_wb);
    }

    #[inline(always)]
    fn convert_gets(&self, request: &Request) {
        debug_assert!(request.keys().len() > 0);
        debug_assert!(self.req_gets(request));
        // 对于单cmd请求：gets转换为get;
        // 对于多个cmd请求（包括1个或多个gets + noop：转换为getkq
        if request.keys().len() == 1 && request.len() == request.keys()[0].len() {
            request.keys()[0].update_u8(PacketPos::Opcode as usize, OP_CODE_GET);
        } else {
            for cmd in request.keys().iter() {
                cmd.update_u8(PacketPos::Opcode as usize, OP_CODE_GETKQ);
            }
        }
    }

    #[inline(always)]
    fn filter_by_key<'a, R>(&self, req: &Request, mut resp: R) -> Option<Request>
    where
        R: Iterator<Item = &'a Response>,
    {
        debug_assert!(req.operation() == Operation::Get || req.operation() == Operation::MGet);
        debug_assert!(req.keys().len() > 0);
        if self.is_single_get(req) {
            if let Some(response) = resp.next() {
                if response.status_ok() && !response.noop() {
                    return None;
                }
            }
            return Some(req.clone());
        }
        // 有多个key
        let found_ids = self.ids_response(resp, req.keys().len());
        let mut cmd = Vec::with_capacity(req.len());
        let mut keys_slice = Vec::with_capacity(req.keys().len());
        // 遍历所有的请求key，如果response中没有，则写入到command中
        for cmd_i in req.keys() {
            if let Some(id) = cmd_i.id() {
                debug_assert!(id.len() > 0);
                if !found_ids.contains_key(&id.into()) {
                    // 写入的是原始的命令, 不是key
                    cmd_i.copy_to_vec(&mut cmd);
                    keys_slice.push(cmd_i.clone());
                }
            }
        }
        if keys_slice.len() > 0 {
            if req.last_key().quite_get() {
                // 写入noop请求
                req.take_noop().copy_to_vec(&mut cmd);
            } else {
                // 设置最后一个key的op_code, 与request的最后一个key的opcode保持一致即可
                let last_len = unsafe { keys_slice.get_unchecked(keys_slice.len() - 1).len() };
                let last_op_pos = cmd.len() - last_len + PacketPos::Opcode as usize;
                cmd[last_op_pos] = req.last_key().op();
            }

            // 业务输入的id可能会重复，导致断言失败
            //debug_assert_eq!(new.keys().len() + found_ids.len(), req.keys().len());
            Some(Request::from_request(cmd, keys_slice, req))
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
        R: Iterator<Item = &'a Response>,
    {
        let (mut left, _) = r.size_hint();
        for response in r {
            // 最后一个请求，不需要做变更，直接写入即可。
            left -= 1;
            if left == 0 {
                w.write(response);
                break;
            }
            // 不是最后一个请求，则处理response的最后一个key
            if response.keys().len() > 0 {
                let last = response.last_key();
                debug_assert_ne!(last.op(), OP_CODE_NOOP);
                match last.op() {
                    OP_CODE_GETQ | OP_CODE_GETKQ => {
                        // 最后一个请求是NOOP，只写入前面的请求
                        let pre = response.sub_slice(0, response.len() - HEADER_LEN);
                        w.write(&pre);
                    }
                    // 把GETK/GET请求，转换成Quite请求。
                    OP_CODE_GETK | OP_CODE_GET => {
                        let op = if last.key_len() == 0 {
                            [response.at(0), OP_CODE_GETQ]
                        } else {
                            [response.at(0), OP_CODE_GETKQ]
                        };
                        w.write(&op[..].into());
                        w.write(&response.sub_slice(2, response.len() - 2));
                    }
                    _ => w.write(response),
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
    // 轮询response，找出本次查询到的keys.
    // keys_response只在filter_by_key调用，为了解析未获取返回值的请求。
    // 只有multiget请求才会调用到该方法
    #[inline(always)]
    fn ids_response<'a, T>(&self, resp: T, exptects: usize) -> HashMap<RingSlice, ()>
    where
        T: Iterator<Item = &'a Response>,
    {
        let mut ids = HashMap::with_capacity(exptects * 3 / 2);
        // 解析response中的key
        for one_respone in resp {
            for cmd in one_respone.keys() {
                if cmd.status_ok() {
                    if let Some(id) = cmd.id() {
                        ids.insert(id, ());
                    }
                }
            }
        }
        debug_assert!(ids.len() <= exptects);
        return ids;
    }
}
