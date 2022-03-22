mod command;
mod flag;
mod packet;
//mod token;

use crate::{
    redis::command::PADDING_RSP_TABLE, Command, Commander, Error, Flag, HashedCommand, Protocol,
    RequestProcessor, Result, Stream,
};
use ds::RingSlice;
use flag::RedisFlager;
use packet::Packet;
use sharding::hash::Hash;

use crate::Utf8;

// redis 协议最多支持10w个token
//const MAX_TOKEN_COUNT: usize = 100000;
//// 最大消息支持1M
//const MAX_MSG_LEN: usize = 1000000;

#[derive(Clone, Default)]
pub struct Redis;

impl Redis {
    #[inline]
    fn parse_request_inner<S: Stream, H: Hash, P: RequestProcessor>(
        &self,
        stream: &mut S,
        alg: &H,
        process: &mut P,
    ) -> Result<()> {
        // TODO 先保留到2022.12，用于快速定位协议问题 fishermen
        if log::log_enabled!(log::Level::Debug) {
            log::debug!("+++ rec req:{:?}", from_utf8(&stream.slice().to_vec()));
        }

        let mut packet = packet::RequestPacket::new(stream);
        while packet.available() {
            packet.parse_bulk_num()?;
            packet.parse_cmd()?;
            let cfg = command::get_cfg(packet.op_code())?;
            let mut hash;
            if cfg.multi {
                packet.multi_ready();
                while packet.has_bulk() {
                    // take会将first变为false, 需要在take之前调用。
                    let bulk = packet.bulk();
                    let first = packet.first();
                    assert!(cfg.has_key);
                    let key = packet.parse_key()?;
                    hash = calculate_hash(alg, &key);
                    if cfg.has_val {
                        packet.ignore_one_bulk()?;
                    }
                    let kv = packet.take();
                    let req = cfg.build_request(hash, bulk, first, kv.data());
                    process.process(req, packet.complete());
                }
            } else {
                if cfg.has_key {
                    let key = packet.parse_key()?;
                    hash = calculate_hash(alg, &key);
                } else {
                    hash = default_hash();
                }
                packet.ignore_all_bulks()?;
                let flag = cfg.flag();
                let cmd = packet.take();
                let req = HashedCommand::new(cmd, hash, flag);
                process.process(req, true);
            }
        }
        Ok(())
    }

    // TODO 临时测试设为pub，测试完毕后去掉pub fishermen
    #[inline]
    pub fn num_skip_all(&self, data: &RingSlice, mut oft: &mut usize) -> Result<Option<Command>> {
        let mut bulk_count = data.num(&mut oft)?;
        while bulk_count > 0 {
            if *oft >= data.len() {
                return Err(crate::Error::ProtocolIncomplete);
            }
            match data.at(*oft) {
                b'$' => {
                    data.num_and_skip(&mut oft)?;
                }
                b'*' => {
                    self.num_skip_all(data, oft)?;
                }
                _ => {
                    log::info!(
                        "unsupport rsp:{:?}, pos: {}/{}",
                        data.utf8(),
                        oft,
                        bulk_count
                    );
                    panic!("not supported in num_skip_all");
                }
            }
            // data.num_and_skip(&mut oft)?;
            bulk_count -= 1;
        }
        Ok(None)
    }

    #[inline]
    fn parse_response_inner<S: Stream>(&self, s: &mut S) -> Result<Option<Command>> {
        let data = s.slice();
        log::debug!("+++ will parse rsp:{:?}", from_utf8(&data.to_vec()));

        if data.len() >= 2 {
            let mut oft = 0;
            match data.at(0) {
                b'-' | b':' | b'+' => data.line(&mut oft)?,
                b'$' => {
                    let _num = data.num_and_skip(&mut oft)?;
                }
                b'*' => {
                    // let mut bulk_count = data.num(&mut oft)?;
                    // while bulk_count > 0 {
                    //     data.num_and_skip(&mut oft)?;
                    //     bulk_count -= 1;
                    // }
                    self.num_skip_all(&data, &mut oft)?;
                }
                _ => {
                    log::info!("not supported:{:?}, {:?}", data.utf8(), data);
                    panic!("not supported");
                }
            }
            assert!(oft <= data.len());
            let mem = s.take(oft);
            let mut flag = Flag::new();
            // redis不需要重试
            flag.set_status_ok(true);
            return Ok(Some(Command::new(flag, mem)));
        }
        Ok(None)
    }
}

impl Protocol for Redis {
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

    // 为每一个req解析一个response
    #[inline]
    fn parse_response<S: Stream>(&self, data: &mut S) -> Result<Option<Command>> {
        match self.parse_response_inner(data) {
            Ok(cmd) => Ok(cmd),
            Err(Error::ProtocolIncomplete) => Ok(None),
            e => e,
        }
    }
    #[inline]
    fn write_response<C: Commander, W: crate::Writer>(&self, ctx: &mut C, w: &mut W) -> Result<()> {
        let req = ctx.request();
        let op_code = req.op_code();
        let cfg = command::get_cfg(op_code)?;
        let response = ctx.response();
        if !cfg.multi {
            // 对于hscan，虽然是单个key，也会返回*2 fishermen
            // assert_ne!(response.data().at(0), b'*');
            w.write_slice(response.data(), 0)
        } else {
            let ext = req.ext();
            let first = ext.mkey_first();
            if first || cfg.need_bulk_num {
                if first && cfg.need_bulk_num {
                    w.write_u8(b'*')?;
                    w.write(ext.key_count().to_string().as_bytes())?;
                    w.write(b"\r\n")?;
                }
                w.write_slice(response.data(), 0)
            } else {
                // 有些请求，如mset，不需要bulk_num,说明只需要返回一个首个key的请求即可。
                // mset always return +OK
                // https://redis.io/commands/mset
                Ok(())
            }
        }
    }
    #[inline]
    fn write_no_response<W: crate::Writer>(&self, req: &HashedCommand, w: &mut W) -> Result<()> {
        let rsp_idx = req.ext().padding_rsp() as usize;
        assert!(rsp_idx < PADDING_RSP_TABLE.len());
        let rsp = *PADDING_RSP_TABLE.get(rsp_idx).unwrap();
        // TODO 先保留到2022.12，用于快速定位协议问题 fishermen
        if log::log_enabled!(log::Level::Debug) {
            log::debug!("+++ will write no rsp. req:{}", req);
        }
        if rsp.len() > 0 {
            w.write(rsp.as_bytes())
        } else {
            // quit，先发+OK，再返回err
            assert_eq!(rsp_idx, 0);
            let ok_rs = PADDING_RSP_TABLE.get(1).unwrap().as_bytes();
            w.write(ok_rs)?;
            Err(crate::Error::Quit)
        }
    }
}

use std::{
    str::from_utf8,
    sync::atomic::{AtomicI64, Ordering},
};
static AUTO: AtomicI64 = AtomicI64::new(0);
// 避免异常情况下hash为0，请求集中到某一个shard上。
// hash正常情况下可能为0?
#[inline]
fn calculate_hash<H: Hash>(alg: &H, key: &RingSlice) -> i64 {
    if key.len() == 0 {
        default_hash()
    } else {
        alg.hash(key)
    }
}

#[inline]
fn default_hash() -> i64 {
    AUTO.fetch_add(1, Ordering::Relaxed)
}
