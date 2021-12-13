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
    fn need_check_master(&self) -> bool {
        false
    }
    #[inline(always)]
    fn parse_request(&self, req: Slice) -> Result<Option<Request>> {
        let split_req = req.split("\r\n".as_ref());
        let first_line = split_req[0].data().to_vec();
        let mut first_line_vec = Vec::new();
        for i in 1..first_line.len() {
            first_line_vec.push(first_line[i]);
        }
        let split_count = String::from_utf8(first_line_vec)
            .unwrap()
            .parse::<usize>()
            .unwrap();
        if split_req.len() < (split_count * 2 + 1) {
            return Ok(None);
        }
        let cmd = String::from_utf8(split_req[2].data().to_vec())
            .unwrap()
            .to_lowercase();
        let op = {
            match cmd.as_str() {
                "get" => Operation::Get,
                "set" => Operation::Store,
                "select" => Operation::Meta,
                "ping" => Operation::Meta,
                "mget" => Operation::MGet,
                "quit" => Operation::Quit,
                _ => Operation::Other,
            }
        };

        let (keys, read) = self.parse_keys_and_length(op, split_count, &split_req);
        unsafe {
            log::debug!(
                "parsed req: {:?}",
                String::from_utf8_unchecked(req.to_vec())
            );
        }
        Ok(Some(Request::from(
            req.sub_slice(0, read),
            op,
            keys.clone(),
        )))
    }
    #[inline]
    fn sharding(
        &self,
        req: &Request,
        shard: &Sharding,
    ) -> (Vec<(usize, Request)>, Vec<Vec<usize>>) {
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
                let single_shard = sharded.get(0).unwrap();
                let mut positions = Vec::new();
                for i in single_shard {
                    positions.push(i.clone());
                }
                return (ret, vec![positions]);
            }
            let mut sharded_req = Vec::with_capacity(sharded.len());
            for (s_idx, indice) in sharded.iter().enumerate() {
                if indice.is_empty() {
                    continue;
                }
                let mut part_num = 1 as usize;
                let mut keys: Vec<Slice> = Vec::with_capacity(indice.len() + 1);
                let mut cmd = Vec::with_capacity(req.len());
                let cmd_head = String::from("*") + &*(indice.len() + 1).to_string() + "\r\n";
                cmd.append(&mut Vec::from(cmd_head));
                cmd.append(&mut Vec::from("$4\r\nmget\r\n"));
                for idx in indice.iter() {
                    part_num = part_num + 1;
                    debug_assert!(*idx < klen);
                    let single_key = req.keys().get_unchecked(*idx);
                    let key_offset = cmd.len() + 1;
                    let item = String::from("$")
                        + &*single_key.len().to_string()
                        + "\r\n"
                        + &*String::from_utf8(single_key.data().to_vec()).unwrap()
                        + "\r\n";
                    cmd.append(&mut Vec::from(item));
                    keys.push(Slice::new(
                        cmd.as_ptr().offset(key_offset as isize) as usize,
                        single_key.len(),
                    ));
                }
                let new = Request::from_request(cmd, keys, req);
                sharded_req.push((s_idx, new));
            }
            (sharded_req, sharded)
        }
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
    // redis 没有get请求是必须请求master
    fn req_gets(&self, request: &Request) -> bool {
        // let op = self.parse_operation(request);
        // op == Command::Gets
        false
    }
    fn req_cas_or_add(&self, request: &Request) -> bool {
        // let op = self.parse_operation(request);
        // op == Command::Cas || op == Command::Add
        false
    }
    #[inline]
    fn parse_response(&self, response: &RingSlice) -> Option<Response> {
        let mut keys = vec![];
        let mut data = response.clone();
        let mut end_with_cr_lf = response.data().ends_with(&*Vec::from("\r\n"));
        let response_lines = response.split("\r\n".as_ref());
        if response_lines.len() == 1 && !end_with_cr_lf {
            return None;
        }
        let first_line = String::from_utf8(response_lines[0].data()).unwrap();
        if RedisResp2::is_single_line(first_line.clone()) {
            data = response.sub_slice(0, response_lines[0].len() + 2);
        } else if RedisResp2::is_bulk_string(first_line.clone()) {
            if (response_lines.len() == 2 && end_with_cr_lf) || response_lines.len() > 2 {
                let len = response_lines[0].len() + 2 + response_lines[1].len() + 2;
                data = response.sub_slice(0, len);
            } else {
                return None;
            }
        } else if RedisResp2::is_array(first_line.clone()) {
            let mut len = 0 as usize;
            let element_count = RedisResp2::get_array_element_count(first_line.clone());
            let mut parsed_element_count = 0 as usize;
            let mut array_parsed = false;
            let mut is_first_line = true;
            let mut hold_element_count = false;
            let mut line_count = 0 as usize;
            let total_count = response_lines.len();
            for response_line in response_lines {
                line_count += 1;
                if line_count >= total_count && !end_with_cr_lf {
                    break;
                }
                if is_first_line {
                    len = len + response_line.len() + 2;
                    is_first_line = false;
                    continue;
                }
                len = len + response_line.len() + 2;
                if !hold_element_count {
                    parsed_element_count += 1;
                } else {
                    hold_element_count = false;
                }
                if RedisResp2::is_bulk_string(String::from_utf8(response_line.data()).unwrap()) {
                    hold_element_count = true;
                    continue;
                }
                if parsed_element_count >= element_count {
                    array_parsed = true;
                    break;
                }
            }
            if array_parsed {
                data = response.sub_slice(0, len);
            } else {
                return None;
            }
        }
        Some(Response::from(data, Operation::Other, keys))
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
    fn write_response<'a, R, W>(&self, r: R, w: &mut W, indexes: Vec<Vec<usize>>)
    where
        W: crate::BackwardWrite,
        R: Iterator<Item = &'a Response>,
    {
        let mut merge_results = !indexes.is_empty();
        let (mut left, _) = r.size_hint();
        let mut i = 0;
        let mut results_vec: Vec<Vec<u8>> = vec![];
        let mut result_vec_max = 0 as usize;
        for response in r {
            left -= 1;
            if !merge_results {
                if left == 0 {
                    w.write(response);
                    break;
                }
            } else {
                let response_lines = response.data().split("\r\n".as_ref());

                let first_line = String::from_utf8(response_lines[0].data().to_vec()).unwrap();
                let split_count = RedisResp2::get_array_element_count(first_line);
                let index = indexes[i].clone();
                debug_assert_eq!(split_count, index.len());
                let mut j = 1;
                let mut items_count = 0;
                while j < response_lines.len() {
                    let single_line = response_lines[j].data().to_vec();
                    let single_line_string = String::from_utf8(single_line.clone()).unwrap();
                    let my_index = index[items_count];
                    while result_vec_max <= my_index {
                        results_vec.push(vec![]);
                        result_vec_max = result_vec_max + 1;
                    }
                    if RedisResp2::is_single_line(single_line_string.clone())
                        || RedisResp2::is_array(single_line_string.clone())
                    {
                        let single_result = results_vec.get_mut(my_index).unwrap();
                        single_result.append(&mut single_line.clone());
                        single_result.append(&mut Vec::from("\r\n"));
                    } else {
                        let mut single_result = results_vec.get_mut(my_index).unwrap();
                        single_result.append(&mut single_line.clone());
                        single_result.append(&mut Vec::from("\r\n"));
                        j = j + 1;
                        if j < response_lines.len() {
                            single_result.append(&mut response_lines[j].data().to_vec().clone());
                            single_result.append(&mut Vec::from("\r\n"));
                        }
                    }
                    j = j + 1;
                    items_count = items_count + 1;
                }
                if left == 0 {
                    let mut total_len = 0 as usize;
                    for result in &results_vec {
                        total_len += result.len();
                    }
                    let mut response_data = Vec::with_capacity(total_len + 10);
                    let first_response_line =
                        String::from("*") + &*(results_vec.len()).to_string() + "\r\n";
                    response_data.append(&mut Vec::from(first_response_line));
                    for mut result in results_vec {
                        response_data.append(&mut result);
                    }
                    let response_slice = RingSlice::from(
                        response_data.as_ptr(),
                        response_data.len().next_power_of_two(),
                        0,
                        response_data.len(),
                    );
                    let response = Response::from(response_slice, Operation::MGet, vec![]);
                    w.write(&*response);
                    break;
                }
            }
            i = i + 1;
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

    fn parse_keys_and_length(
        &self,
        op: Operation,
        item_count: usize,
        split_req: &Vec<Slice>,
    ) -> (Vec<Slice>, usize) {
        let mut keys = Vec::with_capacity(item_count);
        let mut has_multi_keys = false;

        if op == Operation::Meta || op == Operation::Other {
            //meta命令不一定有key，把命令词放进去
            keys.push(split_req[2].clone());
        } else if op == Operation::Get || op == Operation::Store {
            keys.push(split_req[4].clone());
        } else {
            has_multi_keys = true;
        }

        let mut read = 0 as usize;
        let mut key_pos = 4;
        for i in 0..split_req.len() {
            if i >= item_count * 2 + 1 {
                break;
            }
            let single_row = split_req.get(i).unwrap();

            read = read + 2 + single_row.len();
            if i == key_pos && has_multi_keys {
                keys.push(single_row.clone());
                key_pos = key_pos + 2;
            }
        }
        (keys, read)
    }

    fn is_single_line(line: String) -> bool {
        line.starts_with("+")
            || line.starts_with("-")
            || line.starts_with(":")
            || line.starts_with("$-1")
    }

    fn is_bulk_string(line: String) -> bool {
        line.starts_with("$") && !line.starts_with("$-1")
    }

    fn is_array(line: String) -> bool {
        line.starts_with("*")
    }

    fn get_array_element_count(line: String) -> usize {
        let line_vec = Vec::from(line);
        let mut first_line_vec = Vec::new();
        for i in 1..line_vec.len() {
            first_line_vec.push(line_vec[i]);
        }
        let split_count = String::from_utf8(first_line_vec)
            .unwrap()
            .parse::<usize>()
            .unwrap();
        split_count
    }
}
