mod error;
mod reqpacket;
mod rsppacket;

use std::collections::HashMap;

use crate::memcache::Command;
use crate::request::Request;
use crate::{Error, Flag, HashedCommand, Protocol, RequestProcessor, Result, Stream, Utf8};

use ds::RingSlice;
use sharding::hash::Hash;

use self::reqpacket::RequestPacket;

#[derive(Clone)]
pub struct McqText;

impl McqText {
    #[inline]
    fn parse_request_inner<S: Stream, H: Hash, P: RequestProcessor>(
        &self,
        stream: &mut S,
        alg: &H,
        process: &mut P,
    ) -> Result<()> {
        if log::log_enabled!(log::Level::Debug) {
            log::debug!("+++ rec mcq req:{:?}", stream.slice().utf8());
        }

        let packet = RequestPacket::new(stream);
        let state = packet.state();
        while packet.available() {
            packet.parse_req()?;
            let operation = packet.operation();

            let mut flag = Flag::from_op(0u16, operation);

            // mcq 总是需要重试，确保所有消息写成功
            // flag.set_try_next_type(req.try_next_type());
            flag.set_try_next_type(crate::TryNextType::TryNext);
            // mcq 只需要写一次成功即可，不存在noreply(即只sent) req
            flag.set_sentonly(false);
            // 是否内部请求,不发往后端，如quit
            flag.set_noforward(packet.noforward());
            let cmd = packet.take();
            let req = HashedCommand::new(cmd, 0, flag);
            process.process(req, true);
        }
        Ok(())
    }

    #[inline]
    fn parse_response_inner<S: Stream>(&self, s: &mut S) -> Result<Option<Command>> {
        let data = s.slice();
        log::debug!("+++ will parse mcq rsp: {:?}", data.utf8());
    }
}

impl Protocol for McqText {
    #[inline]
    fn parse_request<S: Stream, H: Hash, P: RequestProcessor>(
        &self,
        stream: &mut S,
        alg: &H,
        process: &mut P,
    ) -> Result<()> {
        match self.parse_request_inner(stream, alg, process) {
            Ok(_) => Ok(()),
            Err(Error::ProtocolIncomplete) => Ok(()),
            e => e,
        }
    }

    fn parse_response<S: Stream>(&self, data: &mut S) -> Result<Option<Command>> {
        match self.parse_response_inner(data) {
            Ok(cmd) => Ok(cmd),
            Err(Error::ProtocolIncomplete) => Ok(None),
            e => e,
        }
    }
}

impl MemcacheText {
    pub fn new() -> Self {
        MemcacheText
    }
    #[inline]
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
    #[inline]
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
