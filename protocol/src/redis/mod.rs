mod command;
mod error;
mod flag;
mod packet;
//mod token;

use crate::{
    redis::command::{CommandProperties, SWALLOWED_CMD_HASHKEYQ},
    redis::command::{PADDING_RSP_TABLE, SWALLOWED_CMD_HASHRANDOMQ},
    redis::packet::RequestPacket,
    Command, Commander, Error, Flag, HashedCommand, Protocol, RequestProcessor, Result, Stream,
};
use ds::RingSlice;
use error::*;
use flag::RedisFlager;
use packet::Packet;
use sharding::hash::Hash;

#[allow(unused_imports)]
use crate::Utf8;
use rand;

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
        // 一个指令开始处理，可以重复进入

        // TODO 先保留到2022.12，用于快速定位协议问题 fishermen
        log::debug!("+++ rec req:{:?}", stream.slice().utf8());

        let mut packet = packet::RequestPacket::new(stream);
        while packet.available() {
            // 先尝试parse master
            let master_only = packet.parse_cmd_layer()?;
            packet.parse_bulk_num()?;
            packet.parse_cmd()?;
            // 对于不支持的cmd，记录协议
            let cfg = match command::get_cfg(packet.op_code()) {
                Ok(cfg) => cfg,
                Err(super::Error::ProtocolNotSupported) => {
                    log::warn!("+++ found unsupported req:{:?}", stream.slice().utf8());
                    return Err(super::Error::ProtocolNotSupported);
                }
                Err(e) => return Err(e),
            };
            let mut hash;
            if cfg.swallowed {
                // 优先处理swallow吞噬指令: hashkeyq/hashrandomq
                self.parse_swallow_cmd(cfg, &mut packet, alg)?;
                continue;
            } else if cfg.multi {
                packet.multi_ready();
                while packet.has_bulk() {
                    // take会将first变为false, 需要在take之前调用。
                    let bulk = packet.bulk();
                    let first = packet.first();
                    assert!(cfg.has_key);

                    // 不管是否使用当前cmd的key来计算hash，都需要解析出一个key
                    let key = packet.parse_key()?;

                    // mutli cmd 也支持swallow指令
                    if packet.reserved_hash() != 0 {
                        // 使用hashkey直接指定了hash
                        hash = packet.reserved_hash();
                        log::info!(
                            "+++ use direct hash for multi cmd: {:?}",
                            packet.inner_data().utf8()
                        )
                    } else {
                        hash = calculate_hash(alg, &key);
                    }

                    if cfg.has_val {
                        packet.ignore_one_bulk()?;
                    }
                    let kv = packet.take();
                    let req = cfg.build_request(hash, bulk, first, master_only, kv.data());
                    process.process(req, packet.complete());
                }
            } else {
                let mut flag = cfg.flag();
                if master_only {
                    flag.set_master_only();
                }

                if packet.reserved_hash() != 0 {
                    // 使用hashkey直接指定了hash
                    hash = packet.reserved_hash();
                    flag.set_direct_hash(true);
                } else if cfg.need_reserved_hash {
                    return Err(RedisError::ReqInvalid.error());
                } else if cfg.has_key {
                    let key = packet.parse_key()?;
                    hash = calculate_hash(alg, &key);
                } else {
                    hash = default_hash();
                }
                packet.ignore_all_bulks()?;

                let cmd = packet.take();
                let req = HashedCommand::new(cmd, hash, flag);
                process.process(req, true);

                // 如果是指示下一个cmd hash的特殊指令，需要保留hash
                if cfg.reserve_hash {
                    debug_assert!(cfg.has_key);
                    // 执行到这里，需要保留指示key的，目前只有hashkey
                    debug_assert_eq!(cfg.name, command::DIST_CMD_HASHKEY);
                    packet.update_reserved_hash(hash);
                }
            }

            // 至此，一个指令处理完毕
        }
        Ok(())
    }

    // 解析待吞噬的cmd，目前swallowed cmds有hashkey、hashrandomq这2个，后续还有扩展就在这里加 fishermen
    fn parse_swallow_cmd<S: Stream, H: Hash>(
        &self,
        cfg: &CommandProperties,
        packet: &mut RequestPacket<S>,
        alg: &H,
    ) -> Result<()> {
        debug_assert!(cfg.swallowed);
        match cfg.name {
            // hashkeyq
            SWALLOWED_CMD_HASHKEYQ => {
                let key = packet.parse_key()?;
                let hash: i64;
                // 如果key为-1，需要把指令发送到所有分片，但只返回一个分片的响应
                if key.len() == 2 && key.at(0) == ('-' as u8) && key.at(1) == ('1' as u8) {
                    log::info!("+++ will broadcast: {:?}", packet.inner_data().utf8());
                    hash = crate::MAX_DIRECT_HASH;
                } else {
                    hash = calculate_hash(alg, &key);
                }

                // 记录reserved hash，为下一个指令使用
                packet.update_reserved_hash(hash);
            }
            // "hashrandomq"
            SWALLOWED_CMD_HASHRANDOMQ => {
                // 虽然hash名义为i64，但实际当前均为u32
                let hash = rand::random::<u32>();
                // 记录reserved hash，为下一个指令使用
                packet.update_reserved_hash(hash as i64);
            }
            _ => {
                debug_assert!(false);
                log::warn!("should not come here![hashkey?]");
            }
        }

        // 吞噬掉整个cmd，准备处理下一个cmd fishermen
        packet.ignore_all_bulks()?;
        // 吞噬/trim掉当前 cmd data，准备解析下一个cmd
        packet.trim_swallowed_cmd()?;

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
        log::debug!("+++ will parse rsp:{:?}", data.utf8());

        if data.len() >= 2 {
            let mut oft = 0;
            match data.at(0) {
                b'-' | b':' | b'+' => data.line(&mut oft)?,
                b'$' => {
                    let _num = data.num_and_skip(&mut oft)?;
                }
                b'*' => {
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

                // 对于每个key均需要响应，且响应是异常的场景，返回nil，否则继续返回原响应
                if cfg.need_bulk_num && response.data().at(0) == b'-' && cfg.nil_rsp > 0 {
                    let nil = *PADDING_RSP_TABLE.get(cfg.nil_rsp as usize).unwrap();
                    log::debug!(
                        "+++ write to client nil: {:?}, ignore:{:?}",
                        nil,
                        response.data().utf8()
                    );
                    return w.write(nil.as_bytes());
                }

                return w.write_slice(response.data(), 0);
            } else {
                // 有些请求，如mset，不需要bulk_num,说明只需要返回一个首个key的请求即可。
                // mset always return +OK
                // https://redis.io/commands/mset
                Ok(())
            }
        }
    }

    // dist_fn 用于类似hashkey、keyshard等指令，计算指令对应的分片索引
    #[inline]
    fn write_no_response<W: crate::Writer, F: Fn(i64) -> usize>(
        &self,
        req: &HashedCommand,
        w: &mut W,
        dist_fn: F,
    ) -> Result<()> {
        let rsp_idx = req.ext().padding_rsp() as usize;
        assert!(rsp_idx < PADDING_RSP_TABLE.len());

        // check cmd需要额外构建rsp，目前只有hashkey、keyshard两种dist指令需要构建
        let cfg = command::get_cfg(req.op_code())?;
        let rsp = match cfg.name {
            command::DIST_CMD_HASHKEY => {
                let shard = dist_fn(req.hash());
                format!(":{}\r\n", shard)
            }
            command::DIST_CMD_KEYSHARD => {
                let mut bulk_str: String = String::from("");
                if cfg.multi && cfg.need_bulk_num {
                    let ext = req.ext();
                    let first = ext.mkey_first();
                    if first {
                        if first && cfg.need_bulk_num {
                            bulk_str = format!("*{}\r\n", ext.key_count());
                        }
                    }
                }

                let shard_str = dist_fn(req.hash()).to_string();
                format!("{}${}\r\n{}\r\n", bulk_str, shard_str.len(), shard_str)
            }
            _ => {
                // 对于multi且需要返回rsp数量的请求，按标准协议返回，并返回nil值
                let mut nil_rsp = false;
                if cfg.multi {
                    let ext = req.ext();
                    let first = ext.mkey_first();
                    if first || cfg.need_bulk_num {
                        if first && cfg.need_bulk_num {
                            w.write_u8(b'*')?;
                            w.write(ext.key_count().to_string().as_bytes())?;
                            w.write(b"\r\n")?;
                        }

                        // 对于每个key均需要响应，且响应是异常的场景，返回nil，否则继续返回原响应
                        if cfg.need_bulk_num && cfg.nil_rsp > 0 {
                            nil_rsp = true;
                        }
                    }
                }
                if nil_rsp {
                    let nil = *PADDING_RSP_TABLE.get(cfg.nil_rsp as usize).unwrap();
                    log::debug!("+++ write client nil/{} for:{:?}", nil, req.data().utf8());
                    nil.to_string()
                } else {
                    let padding_rsp = *PADDING_RSP_TABLE.get(rsp_idx).unwrap();
                    padding_rsp.to_string()
                }
            }
        };

        // TODO 先保留到2022.12，用于快速定位协议问题 fishermen
        log::debug!(
            "+++ will write noforward rsp/{:?} for req:{:?}",
            rsp,
            req.data().utf8()
        );
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

    #[inline]
    fn build_writeback_request<C: Commander>(&self, ctx: &mut C, _: u32) -> Option<HashedCommand> {
        let hash_cmd = ctx.request_mut();
        debug_assert!(hash_cmd.direct_hash());

        // hash idx 放到topo.send 中处理
        // let idx_hash = hash_cmd.hash() - 1;
        // hash_cmd.update_hash(idx_hash);
        hash_cmd.set_ignore_rsp(true);
        None
    }
}

use std::sync::atomic::{AtomicI64, Ordering};
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
