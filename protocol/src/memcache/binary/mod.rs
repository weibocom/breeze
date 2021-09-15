use std::collections::HashMap;
use std::io::{Cursor, Error, ErrorKind, Result};

use ds::{RingSlice, Slice};
use sharding::Sharding;

use crate::{MetaType, Operation, Protocol, Request, Response};

mod meta;
use meta::*;

mod packet;
use packet::*;

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
    fn sharding(&self, req: &Request, shard: &Sharding) -> Vec<(usize, Request)> {
        // 只有multiget才有分片
        // debug_assert_eq!(req.operation(), Operation::Gets);
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
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "not keys found in response",
            ));
        }

        let origin = response.as_ref();
        let mut data = Vec::with_capacity(response.len());
        origin.copy_to_vec(&mut data);

        log::info!(
            "+++++++++++ will build set for origin req: {:?}",
            request.data()
        );
        log::info!("+++++++++++ will build set for resp: {:?}", data);
        // 轮询response，查出所有的子响应
        // let mut req_buf: Vec<u8> = Vec::new();
        let mut requests_wb = Vec::with_capacity(response.keys().len());
        let mut cursor_reader = Cursor::new(data);
        while cursor_reader.position() + 1 < origin.len() as u64 {
            if cursor_reader.position() + HEADER_LEN as u64 == origin.len() as u64 {
                return Ok(requests_wb);
            }

            let resp_packet = packet::parse_response_packet(&mut cursor_reader)?;
            if resp_packet.value.len() == 0 {
                log::info!("found empty response.");
                continue;
            }

            let flags = resp_packet.parse_get_response_flag()?;

            // 构建 set request
            let use_request_key = resp_packet.key.len() == 0 || request.keys().len() == 1;
            let key = if use_request_key {
                debug_assert!(request.keys().len() == 1);
                let mut req_key = Vec::new();
                req_key.extend_from_slice(request.keys()[0].key().data());
                req_key
            } else {
                resp_packet.key
            };
            log::info!("+++++++ use req key/{}, key: {:?}", use_request_key, key);
            let set_req_packet = packet::SetRequest {
                header: PacketHeader {
                    magic: Magic::Request as u8,
                    opcode: Opcode::SetQ as u8,
                    // 对于单个key请求，response中无key，直接使用request中的key
                    // TODO 后续需要改造为通用型支持respons中无key的gets请求
                    key_length: key.len() as u16,
                    extras_length: SET_REQUEST_EXTRATS_LEN as u8,
                    data_type: 0u8,
                    vbucket_id_or_status: 0u16,
                    total_body_length: SET_REQUEST_EXTRATS_LEN as u32
                        + key.len() as u32
                        + resp_packet.value.len() as u32,
                    opaque: 0u32,
                    cas: 0u64,
                },
                store_extras: StoreExtras {
                    flags: flags,
                    expiration: expire_seconds,
                },
                key: key,
                value: resp_packet.value,
            };
            let len = HEADER_LEN + set_req_packet.header.total_body_length as usize;
            let mut set_req_data = Vec::with_capacity(len);
            set_req_packet.write(&mut set_req_data)?;
            let cmds = vec![Slice::from(&set_req_data[0..len])];

            let set_req = Request::from_data(
                set_req_data,
                cmds,
                request.id().clone(),
                true,
                Operation::Store,
            );
            log::info!("++++++++++ build set req: {:?}", set_req.data());
            requests_wb.push(set_req);
        }
        return Ok(requests_wb);
    }

    fn filter_by_key<'a, R>(&self, req: &Request, mut resp: R) -> Option<Request>
    where
        R: Iterator<Item = &'a Response>,
    {
        debug_assert!(req.operation() == Operation::Get || req.operation() == Operation::Gets);
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
                    OP_CODE_GETK | OP_CODE_GET => w.write_on(response, |data| {
                        let op = if last.key_len() == 0 {
                            OP_CODE_GETQ
                        } else {
                            OP_CODE_GETKQ
                        };
                        let last_op_idx = response.len() - last.len() + PacketPos::Opcode as usize;
                        data[last_op_idx] = op;
                        debug_assert_eq!(data.len(), response.len());
                    }),
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
