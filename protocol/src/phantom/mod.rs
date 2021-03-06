// phantom 也走redis协议，只是key格式不同，请求的key格式为hashkey.realkey，解析时，需要进行协议转换
// 为了避免对redis的侵入，独立实现

mod command;
mod error;
mod flag;
mod packet;
//mod token;

use crate::{
    phantom::command::PADDING_RSP_TABLE, Command, Commander, Error, Flag, HashedCommand, Protocol,
    RequestProcessor, Result, Stream,
};
use ds::RingSlice;
use flag::RedisFlager;
use packet::Packet;
use sharding::hash::Hash;

use crate::Utf8;

#[derive(Clone, Default)]
pub struct Phantom;

impl Phantom {
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
            if cfg.multi {
                packet.multi_ready();
                while packet.has_bulk() {
                    // take会将first变为false, 需要在take之前调用。
                    let bulk = packet.bulk();
                    let first = packet.first();
                    assert!(cfg.has_key);
                    // 注意，fullkey格式: $hash_key.$realkey
                    let full_key = packet.parse_key()?;
                    let (hash, real_key) = split_and_calculate_hash(alg, &full_key);
                    debug_assert!(!cfg.has_val);
                    // if cfg.has_val {
                    //     packet.ignore_one_bulk()?;
                    // }
                    // packet里的数据不用，重新构建cmd
                    let _ = packet.take();
                    let req = cfg.build_request_with_key_for_multi(hash, bulk, first, &real_key);
                    process.process(req, packet.complete());
                }
            } else {
                if cfg.has_key {
                    let full_key = packet.parse_key()?;
                    let (hash, real_key) = split_and_calculate_hash(alg, &full_key);
                    packet.ignore_all_bulks()?;

                    // 需要根据key进行重建
                    let _ = packet.take();
                    let req = cfg.build_request_with_key(hash, &real_key);
                    process.process(req, true);
                } else {
                    let hash = default_hash();
                    packet.ignore_all_bulks()?;
                    let flag = cfg.flag();
                    let cmd = packet.take();
                    let req = HashedCommand::new(cmd, hash, flag);
                    process.process(req, true);
                }
                //     packet.ignore_all_bulks()?;
                //     let flag = cfg.flag();
                //     let cmd = packet.take();
                //     let req = HashedCommand::new(cmd, hash, flag);
                //     process.process(req, true);
            }
        }
        Ok(())
    }

    // TODO 临时测试设为pub，测试完毕后去掉pub fishermen
    // 需要支持4种协议格式：（除了-代表的错误类型）
    //    1）* 代表array； 2）$代表bulk 字符串；3）+ 代表简单字符串；4）:代表整型；
    #[inline]
    pub fn num_skip_all(&self, data: &RingSlice, mut oft: &mut usize) -> Result<Option<Command>> {
        let mut bulk_count = data.num(&mut oft)?;
        while bulk_count > 0 {
            if *oft >= data.len() {
                return Err(crate::Error::ProtocolIncomplete);
            }
            match data.at(*oft) {
                b'*' => {
                    self.num_skip_all(data, oft)?;
                }
                b'$' => {
                    data.num_and_skip(&mut oft)?;
                }
                b'+' => data.line(oft)?,
                b':' => data.line(oft)?,
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

        // phantom 在rsp为负数（:-1/-2/-3\r\n），说明请求异常，需要重试
        let mut status_ok = true;
        if data.len() > 2 {
            let mut oft = 0;
            match data.at(0) {
                b':' => {
                    // 负数均重试，避免HA配置差异
                    if data.at(1) == b'-' {
                        status_ok = false;
                    }
                    data.line(&mut oft)?;
                }
                b'-' | b'+' => {
                    // 对于phantom，-/+均需要重试
                    status_ok = false;
                    data.line(&mut oft)?;
                }
                b'$' => {
                    let _num = data.num_and_skip(&mut oft)?;
                }
                b'*' => {
                    self.num_skip_all(&data, &mut oft)?;
                }
                _ => {
                    log::info!("phantom not supported:{:?}, {:?}", data.utf8(), data);
                    panic!("not supported");
                }
            }
            assert!(oft <= data.len());
            let mem = s.take(oft);
            let mut flag = Flag::new();
            flag.set_status_ok(status_ok);

            return Ok(Some(Command::new(flag, mem)));
        }
        Ok(None)
    }
}

impl Protocol for Phantom {
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
// hash正常情况下可能为0
#[inline]
fn split_and_calculate_hash<H: Hash>(alg: &H, full_key: &RingSlice) -> (i64, RingSlice) {
    let delimiter = ['.' as u8; 1];
    if let Some(idx) = full_key.find_sub(0, &delimiter) {
        debug_assert!(idx > 0);
        let hash_key = full_key.sub_slice(0, idx);
        let real_key_len = full_key.len() - idx - 1;
        let real_key = full_key.sub_slice(idx + 1, real_key_len);

        debug_assert!(real_key.len() > 0);
        let hash = alg.hash(&hash_key);
        return (hash, real_key);
    }

    log::warn!("phantom - malform key: {:?}", full_key.utf8());
    (0, full_key.sub_slice(0, full_key.len()))
}

#[inline]
fn default_hash() -> i64 {
    AUTO.fetch_add(1, Ordering::Relaxed)
}
