use std::collections::HashMap;
use std::io::{Result, Write};

use crate::memcache::Command;
use crate::{MetaType, Operation, Protocol, Request, Response};

use ds::{RingSlice, Slice};
use sharding::Sharding;

#[derive(Clone)]
pub struct MemcacheText;
impl Protocol for MemcacheText {
    fn resource(&self) -> crate::Resource {
        crate::Resource::Memcache
    }
    // 当前请求必须不是noreply的
    #[inline]
    fn with_noreply(&self, req: &[u8]) -> Vec<u8> {
        debug_assert_eq!(self._noreply(req), false);
        let req_string = String::from_utf8(Vec::from(req)).unwrap();
        let rows = req_string.split("\r\n").collect::<Vec<&str>>();
        //string的split如果以分隔符结尾会有一个空字符串
        debug_assert_eq!(rows.len(), 3);
        let mut v = vec![0u8; req.len() + " noreply".len()];
        use std::ptr::copy_nonoverlapping as copy;
        unsafe {
            let mut offset = 0 as isize;
            copy(rows[0].as_ptr(), v.as_mut_ptr(), rows[0].len());
            offset += rows[0].len() as isize;
            copy(
                " noreply\r\n".as_ptr(),
                v.as_mut_ptr().offset(offset),
                " noreply\r\n".len(),
            );
            offset += " noreply\r\n".len() as isize;
            copy(
                rows[1].as_ptr(),
                v.as_mut_ptr().offset(offset),
                rows[1].len(),
            );
            offset += rows[1].len() as isize;
            copy("\r\n".as_ptr(), v.as_mut_ptr().offset(offset), "\r\n".len());
        }
        v
    }
    #[inline(always)]
    fn parse_request(&self, req: Slice) -> Result<Option<Request>> {
        let split_req = req.split(" ".as_ref());
        let mut read = 0 as usize;
        let mut keys: Vec<Slice> = vec![];
        let op = {
            match String::from_utf8(split_req[0].data().to_vec())
                .unwrap()
                .to_lowercase()
                .as_str()
            {
                "get" => Operation::Get,
                "gets" => Operation::MGet,
                "set" => Operation::Store,
                "version\r\n" => Operation::Meta,
                _ => Operation::Other,
            }
        };
        //为了防止后边的解析出现逻辑错误，目前get只支持一个key
        if op == Operation::Get && split_req.len() > 2 {
            return Ok(None);
        }

        return if op == Operation::Store {
            let lr_cf_split = req.split("\r\n".as_ref());
            if lr_cf_split.len() < 2 {
                Ok(None)
            } else {
                let key_row = &lr_cf_split[0];
                let value_row = &lr_cf_split[1];

                let key_split = key_row.split(" ".as_ref());
                debug_assert!(key_split.len() >= 5);
                let key = &key_split[1];
                let value_bytes_string = String::from_utf8(Vec::from(key_split[4].data())).unwrap();
                let value_bytes = value_bytes_string.parse::<usize>().unwrap();
                assert_eq!(value_bytes, value_row.len());

                keys.push(key.clone());
                read = read + key_row.len() + "\r\n".len() + value_row.len() + "\r\n".len();
                Ok(Some(Request::from(
                    req.sub_slice(0, read),
                    op,
                    keys.clone(),
                )))
            }
        } else {
            read = read + split_req[0].len();
            if split_req.len() > 1 {
                let mut key_position = 2 as usize;
                if op.eq(&Operation::MGet) {
                    key_position = split_req.len();
                }
                for i in 1..split_req.len() {
                    let key = split_req.get(i).unwrap();
                    read = read + 1 + key.len();
                    if key.ends_with("\r\n".as_ref()) {
                        if i < key_position {
                            keys.push(key.split("\r\n".as_ref()).get(0).unwrap().clone());
                        }
                        return Ok(Some(Request::from(
                            req.sub_slice(0, read),
                            op,
                            keys.clone(),
                        )));
                    } else {
                        if i < key_position {
                            keys.push(key.clone());
                        }
                    }
                }
            }
            Ok(Some(Request::from(
                req.sub_slice(0, read),
                op,
                keys.clone(),
            )))
        };
    }
    #[inline]
    fn sharding(&self, req: &Request, shard: &Sharding) -> Vec<(usize, Request)> {
        // 只有multiget才有分片
        debug_assert_eq!(req.operation(), Operation::MGet);
        unsafe {
            let klen = req.keys().len();
            let mut keys = Vec::with_capacity(klen);
            for key in req.keys() {
                keys.push(key.clone());
            }
            debug_assert!(keys.len() > 0);

            let sharded = shard.shardings(keys);
            if sharded.len() == 1 {
                let mut ret = Vec::with_capacity(1);
                let (s_idx, _) = sharded.iter().enumerate().next().expect("only one shard");
                ret.push((s_idx, req.clone()));
                return ret;
            }
            let mut sharded_req = Vec::with_capacity(sharded.len());
            for (s_idx, indice) in sharded.iter().enumerate() {
                if indice.is_empty() {
                    continue;
                }
                let mut cmd: Vec<u8> = Vec::with_capacity(req.len());
                cmd.write("gets".as_ref()).unwrap();
                let mut keys: Vec<Slice> = Vec::with_capacity(indice.len() + 1);
                for idx in indice.iter() {
                    debug_assert!(*idx < klen);
                    let single_key = req.keys().get_unchecked(*idx);
                    let key_offset = cmd.len() + 1;
                    cmd.append(&mut Vec::from(" "));
                    cmd.append(&mut Vec::from(single_key.data()));
                    keys.push(Slice::new(
                        cmd.as_ptr().offset(key_offset as isize) as usize,
                        single_key.len(),
                    ));
                }
                // 最后一个是noop请求，则需要补充上noop请求
                cmd.append(&mut Vec::from("\r\n"));
                let new = Request::from_request(cmd, keys, req);
                sharded_req.push((s_idx, new));
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
        req.keys().get(0).unwrap().clone()
    }
    fn req_gets(&self, request: &Request) -> bool {
        let op = self.parse_operation(request);
        op == Command::Gets
    }
    fn req_cas_or_add(&self, request: &Request) -> bool {
        let op = self.parse_operation(request);
        op == Command::Cas || op == Command::Add
    }
    // 会一直持续到非quite response，才算结束，而非仅仅用noop判断，以应对getkq...getkq + getk的场景
    #[inline]
    fn parse_response(&self, response: &RingSlice) -> Option<Response> {
        let response_lines = response.split("\r\n".as_ref());
        let mut keys = vec![];
        let mut is_data = false;
        for response_line in response_lines {
            if is_data {
                is_data = false;
            } else {
                if response_line.find_sub(0, "VALUE ".as_ref()).is_some() {
                    let response_items = response_line.split(" ".as_ref());
                    if response_items.len() > 2 {
                        keys.push(response_items[1].clone());
                    }
                    is_data = true;
                }
            }
        }
        Some(Response::from(response.clone(), Operation::Other, keys))
    }

    fn convert_gets(&self, _request: &Request) {
        // ascii protocol need do noth.
        return;
    }

    fn filter_by_key<'a, R>(&self, req: &Request, mut resp: R) -> Option<Request>
    where
        R: Iterator<Item = &'a Response>,
    {
        debug_assert!(req.operation() == Operation::Get || req.operation() == Operation::MGet);
        debug_assert!(req.keys().len() > 0);
        if self.is_single_get(req) {
            if let Some(response) = resp.next() {
                if response.as_ref().find_sub(0, "VALUE ".as_ref()).is_some() {
                    return None;
                }
            }
            return Some(req.clone());
        }
        // 有多个key
        let found_keys = self.keys_response(resp, req.keys().len());
        let mut cmd = Vec::with_capacity(req.len());
        cmd.write("gets".as_ref()).unwrap();
        let mut not_found_keys = Vec::with_capacity(req.keys().len());
        // 遍历所有的请求key，如果response中没有，则写入到command中
        for single_key in req.keys() {
            if !found_keys.contains_key(&single_key.clone().into()) {
                unsafe {
                    let key_offset = cmd.len() + 1;
                    cmd.append(&mut Vec::from(" "));
                    cmd.append(&mut Vec::from(single_key.data()));
                    not_found_keys.push(Slice::new(
                        cmd.as_ptr().offset(key_offset as isize) as usize,
                        single_key.len(),
                    ));
                }
            }
        }
        if not_found_keys.len() > 0 {
            let new = Request::from_request(cmd, not_found_keys, req);
            debug_assert_eq!(new.keys().len() + found_keys.len(), req.keys().len());
            Some(new)
        } else {
            None
        }
    }
    // 需要特殊处理multiget请求。
    // multiget需要将非最后一个请求的END行去除
    #[inline]
    fn write_response<'a, R, W>(&self, r: R, w: &mut W)
    where
        W: crate::BackwardWrite,
        R: Iterator<Item = &'a Response>,
    {
        let (mut left, _) = r.size_hint();
        for response in r {
            left -= 1;
            // 最后一个请求，不需要做变更，直接写入即可。
            if left == 0 {
                w.write(response);
                break;
            }
            // 不是最后一个请求，则处理response的最后一个key
            let kl = response.keys().len();
            if kl > 0 {
                let index_result = response.as_ref().find_sub(0, "END\r\n".as_ref());
                if index_result.is_some() {
                    let index = index_result.unwrap();
                    w.write(&response.sub_slice(0, index));
                }
            }
        }
    }

    //文本协议暂时先返回空
    fn convert_to_writeback_request(
        &self,
        _request: &Request,
        _response: &Response,
        _expire_seconds: u32,
    ) -> Result<Vec<Request>> {
        Ok(Vec::new())
    }
}

impl MemcacheText {
    pub fn new() -> Self {
        MemcacheText
    }
    #[inline(always)]
    fn _noreply(&self, req: &[u8]) -> bool {
        req.ends_with(" noreply\r\n".as_ref())
    }
    // 是否只包含了一个key。只有get请求才会用到
    #[inline]
    fn is_single_get(&self, req: &Request) -> bool {
        let keys = req.keys();
        match keys.len() {
            0 | 1 => true,
            _ => false,
        }
    }
    // 轮询response，找出本次查询到的keys，loop所在的位置
    #[inline(always)]
    fn keys_response<'a, T>(&self, resp: T, exptects: usize) -> HashMap<RingSlice, ()>
    where
        T: Iterator<Item = &'a Response>,
    {
        let mut keys = HashMap::with_capacity(exptects * 3 / 2);
        // 解析response中的key
        for one_response in resp {
            for response_key in one_response.keys() {
                keys.insert(response_key.clone(), ());
            }
        }
        return keys;
    }

    fn parse_operation(&self, request: &Request) -> Command {
        // let req_slice = Slice::from(request.data());
        let split_req = request.split(" ".as_ref());
        match String::from_utf8(split_req[0].data().to_vec())
            .unwrap()
            .to_lowercase()
            .as_str()
        {
            "get" => Command::Get,
            "gets" => Command::Gets,
            "set" => Command::Set,
            "cas" => Command::Cas,
            "add" => Command::Add,
            "version\r\n" => Command::Version,
            _ => {
                log::error!(
                    "found unknown command:{:?}",
                    String::from_utf8(split_req[0].data().to_vec())
                );
                Command::Unknown
            }
        }
    }
}
