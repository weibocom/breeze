pub(crate) mod command;
pub(crate) mod error;
pub(crate) mod flag;
pub use flag::RedisFlager;
pub(crate) mod packet;

use crate::{
    redis::command::CommandType,
    redis::{error::RedisError, packet::RequestPacket},
    Command, Commander, Error, HashedCommand, Metric, MetricItem, MetricName, Protocol,
    RequestProcessor, Result, Stream, Writer,
};
pub use packet::Packet;
use sharding::hash::Hash;

#[derive(Clone, Default)]
pub struct Redis;

impl Redis {
    #[inline]
    fn parse_request_inner<S: Stream, H: Hash, P: RequestProcessor>(
        &self,
        packet: &mut RequestPacket<S>,
        alg: &H,
        process: &mut P,
    ) -> Result<()> {
        // 一个指令开始处理，可以重复进入

        // TODO 先保留到2022.12，用于快速定位协议问题 fishermen
        log::debug!("+++ rec redis req:{:?}", packet.inner_data());
        while packet.available() {
            packet.parse_bulk_num()?;
            let cfg = packet.parse_cmd()?;
            if cfg.multi {
                packet.multi_ready();
                while packet.has_bulk() {
                    // take会将first变为false, 需要在take之前调用。
                    let bulk = packet.bulk();
                    let first = packet.first();
                    debug_assert!(cfg.has_key, "cfg:{}", cfg.name);

                    let flag = packet.flag(cfg);
                    let hash = packet.hash(cfg, alg)?;

                    if cfg.has_val {
                        packet.ignore_one_bulk()?;
                    }
                    let kv = packet.take();
                    let req = cfg.build_request(hash, bulk, first, flag, &kv);
                    process.process(req, packet.complete());
                }
            } else {
                let (flag, hash) = if cfg.effect_on_next_req {
                    packet.proc_effect_on_next_req_cmd(&cfg, alg)?
                } else {
                    (packet.flag(cfg), packet.hash(cfg, alg)?)
                };

                packet.ignore_all_bulks()?;
                let cmd = packet.take();
                if !cfg.swallowed {
                    let req = HashedCommand::new(cmd, hash, flag);
                    process.process(req, true);
                }
            }
            //一个请求结束才会走到这
            packet.clear_status(cfg);
        }
        Ok(())
    }

    #[inline]
    fn parse_response_inner<S: Stream>(
        &self,
        s: &mut S,
        oft: &mut usize,
    ) -> Result<Option<Command>> {
        let data: Packet = s.slice().into();
        log::debug!("+++ will parse redis rsp:{:?}", data);
        data.check_onetoken(*oft)?;

        match data.at(0) {
            b'-' | b':' | b'+' => data.line(oft)?,
            b'$' => {
                *oft += data.num_of_string(oft)? + 2;
            }
            b'*' => data.skip_all_bulk(oft)?,
            _ => {
                log::error!("+++ found malformed redis rsp:{:?}", data);
                return Err(RedisError::RespInvalid.into());
            } // _ => panic!("not supported:{:?}", data),
        }

        Ok((*oft <= data.len()).then(|| Command::from_ok(s.take(*oft))))
    }
}

impl Protocol for Redis {
    #[inline]
    fn config(&self) -> crate::Config {
        crate::Config {
            pipeline: true,
            ..Default::default()
        }
    }
    #[inline]
    fn parse_request<S: Stream, H: Hash, P: RequestProcessor>(
        &self,
        stream: &mut S,
        alg: &H,
        process: &mut P,
    ) -> Result<()> {
        let mut packet = RequestPacket::new(stream);
        match self.parse_request_inner(&mut packet, alg, process) {
            Ok(_) => Ok(()),
            Err(Error::ProtocolIncomplete) => {
                // 如果解析数据不够，提前reserve stream的空间
                packet.reserve_stream_buff();
                Ok(())
            }
            e => {
                log::warn!("redis parsed err: {:?}, req: {:?} ", e, packet.inner_data());
                e
            }
        }
    }

    // 为每一个req解析一个response
    #[inline]
    fn parse_response<S: Stream>(&self, data: &mut S) -> Result<Option<Command>> {
        let mut oft = 0;
        match self.parse_response_inner(data, &mut oft) {
            Ok(cmd) => Ok(cmd),
            Err(Error::ProtocolIncomplete) => {
                //assert!(oft + 3 >= data.len(), "oft:{} => {:?}", oft, data.slice());
                if oft > data.len() {
                    data.reserve(oft - data.len());
                }

                Ok(None)
            }
            e => e,
        }
    }

    // TODO：当前把padding、nil整合成一个，后续考虑如何把spec-rsp也整合进来
    // 发送响应给client：
    //  1 非multi，有rsponse直接发送，否则构建padding or spec-rsp后发送；
    //  2 multi，need-bulk-num为true，有response+ok直接发送，否则构建padding or spec-rsp后发送；
    //  3 multi，need-bulk-num为false，first，有response直接发送，否则构建padding后发送；
    //  4 multi，need-bulk-num为false，非first，不做任何操作；
    #[inline]
    fn write_response<C, W, M, I>(
        &self,
        ctx: &mut C,
        response: Option<&mut Command>,
        w: &mut W,
    ) -> Result<()>
    where
        W: Writer,
        C: Commander<M, I>,
        M: Metric<I>,
        I: MetricItem,
    {
        let request = ctx.request();
        let cfg = command::get_cfg(request.op_code())?;

        if !cfg.multi {
            // 非multi请求,有响应直接返回client，否则构建
            if let Some(rsp) = response {
                w.write_slice(rsp, 0)?;
            } else {
                // 无响应，则根据cmd name构建对应响应
                w.write(cfg.get_padding_rsp())?;
            }

            // quit指令发送完毕后，返回异常断连接
            if cfg.quit {
                return Err(crate::Error::Quit);
            }
        } else {
            // multi请求，如果需要bulk num，对第一个key先返回bulk head；
            // 对第一个key的响应都要返回，但对不需要bulk num的其他key响应，不需要返回，直接吞噬
            let first = request.mkey_first();
            if first || cfg.need_bulk_num {
                if first && cfg.need_bulk_num {
                    w.write_u8(b'*')?;
                    w.write_str_num(request.key_count() as usize)?;
                    w.write(b"\r\n")?;
                }

                // 如果rsp是ok，或者不需要bulk num，直接发送；否则构建rsp or padding rsp
                if let Some(rsp) = response {
                    if rsp.ok() || !cfg.need_bulk_num {
                        w.write_slice(rsp, 0)?;
                        return Ok(());
                    }
                }

                // 构建rsp or padding rsp
                match cfg.cmd_type {
                    // 当前需要单独创建rsp的multi指令只有keyshard
                    CommandType::SpecLocalCmdKeyshard => {
                        let shard = ctx.request_shard().to_string();
                        // format!("${}\r\n{}\r\n", shard_str.len(), shard_str);
                        w.write(b"$")?;
                        w.write(shard.len().to_string().as_bytes())?;
                        w.write(b"\r\n")?;
                        w.write(shard.as_bytes())?;
                        w.write(b"\r\n")?;
                    }
                    _ => w.write(cfg.get_padding_rsp())?,
                };

                // rsp不为ok，对need_bulk_num为true的cmd进行nil convert 统计
                if cfg.need_bulk_num {
                    *ctx.metric().get(MetricName::NilConvert) += 1;
                }
            }
            // 有些请求，如mset，不需要bulk_num,说明只需要返回一个首个key的请求即可；这些响应直接吞噬。
            // mset always return +OK
            // https://redis.io/commands/mset
        }
        Ok(())
    }

    #[inline(always)]
    fn check(&self, _req: &HashedCommand, _resp: &Command) {
        if _resp[0] == b'-' {
            log::error!("+++ check failed for req:{:?}, resp:{:?}", _req, _resp);
        }
    }
}

// tests only
pub use packet::RequestContext;
