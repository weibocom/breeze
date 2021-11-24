use crate::redis::Command;
use crate::{MetaType, Operation, Protocol, Request, Resource, Response};
use ds::{RingSlice, Slice};
use sharding::Sharding;
use std::collections::HashMap;
use std::io::Result;

#[derive(Clone)]
pub struct RedisResp2;
impl Protocol for RedisResp2 {
    fn resource(&self) -> Resource {
        Resource::Redis
    }
    #[inline(always)]
    fn parse_request(&self, req: Slice) -> Result<Option<Request>> {
        let split_req = req.split("\r\n".as_ref());
        let mut split_first_vu8 = Vec::new();
        split_first_vu8.push(split_req[0].data().to_vec()[1]);
        let split_count = String::from_utf8(split_first_vu8)
            .unwrap()
            .parse::<usize>()
            .unwrap();
        if split_req.len() < (split_count * 2 + 1) {
            return Ok(None);
        }
        let mut read = 0 as usize;
        let cmd = String::from_utf8(split_req[2].data().to_vec())
            .unwrap()
            .to_lowercase();
        let op = {
            match cmd.as_str()
            {
                "get" => Operation::Get,
                "set" => Operation::Store,
                "select" => Operation::Meta,
                _ => Operation::Other,
            }
        };
        let mut keys: Vec<Slice> = vec![];

        //if cmd.as_str() == "command" {
        //    return Ok(None);
        //}
        //meta命令不一定有key，把命令词放进去
        if op == Operation::Meta || op == Operation::Other {
            keys.push(split_req[2].clone());
        }
        else {
            keys.push(split_req[4].clone());
        }

        read = read + split_req[0].len();
        for i in 1..split_req.len() {
            if i >= split_count * 2 + 1 {
                break;
            }
            let single_row = split_req.get(i).unwrap();
            read = read + 2 + single_row.len();
        }
        read = read + 2;
        Ok(Some(Request::from(
            req.sub_slice(0, read),
            op,
            keys.clone(),
        )))
    }
    #[inline]
    fn sharding(&self, req: &Request, shard: &Sharding) -> Vec<(usize, Request)> {
        // 只有multiget才有分片
        Vec::new()
    }
    #[inline]
    fn with_noreply(&self, req: &[u8]) -> Vec<u8> {
        let mut v = vec![0u8; req.len()];
        use std::ptr::copy_nonoverlapping as copy;
        unsafe {
            let mut offset = 0 as isize;
            copy(req.as_ptr(), v.as_mut_ptr(), req.len());
        }
        v
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
    #[inline]
    fn parse_response(&self, response: &RingSlice) -> Option<Response> {
        let mut keys = vec![];
        Some(Response::from(response.clone(), Operation::Other, keys))
    }

    fn convert_to_writeback_request(
        &self,
        _request: &Request,
        _response: &Response,
        _expire_seconds: u32,
    ) -> Result<Vec<Request>> {
        Ok(Vec::new())
    }

    fn convert_gets(&self, _request: &Request) {
        // ascii protocol need do noth.
        return;
    }
    //单查询暂不需要
    fn filter_by_key<'a, R>(&self, req: &Request, mut resp: R) -> Option<Request>
    where
        R: Iterator<Item = &'a Response>,
    {
        None
    }

    #[inline]
    fn write_response<'a, R, W>(&self, r: R, w: &mut W)
    where
        W: crate::BackwardWrite,
        R: Iterator<Item = &'a Response>,
    {
        let (mut left, _) = r.size_hint();
        for response in r {
            left -= 1;
            if left == 0 {
                w.write(response);
                break;
            }
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
}

impl RedisResp2 {
    pub fn new() -> Self {
        RedisResp2
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
        let split_req = request.split("\r\n".as_ref());
        match String::from_utf8(split_req[2].data().to_vec())
            .unwrap()
            .to_lowercase()
            .as_str()
        {
            "get" => Command::Get,
            "set" => Command::Set,
            _ => {
                log::error!(
                    "found unknown command:{:?}",
                    String::from_utf8(split_req[2].data().to_vec())
                );
                Command::Unknown
            }
        }
    }
}
