pub(crate) mod command;
pub(crate) mod error;
pub(crate) mod flag;
pub use flag::RedisFlager;
pub(crate) mod packet;
//mod token;

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
                // // 如果是指示下一个cmd hash的特殊指令，需要保留hash
                // if cfg.reserve_hash {
                //     debug_assert!(cfg.has_key, "cfg:{}", cfg.name);
                //     // 执行到这里，需要保留指示key的，目前只有hashkey
                //     debug_assert_eq!(cfg.cmd_type, CommandType::SpecLocalCmdHashkey);
                //     packet.update_reserved_hash(hash);
                // }
            }
            //一个请求结束才会走到这
            packet.clear_status(cfg);
        }
        Ok(())
    }

    // 解析待吞噬的cmd，解析后会trim掉吞噬指令，消除吞噬指令的影响，目前swallowed cmds有master、hashkeyq、hashrandomq这几个，后续还有扩展就在这里加 fishermen
    // fn parse_swallow_cmd<S: Stream, H: Hash>(
    //     &self,
    //     cfg: &CommandProperties,
    //     packet: &mut RequestPacket<S>,
    //     alg: &H,
    // ) -> Result<()> {
    //     debug_assert!(cfg.swallowed, "cfg:{}", cfg.name);
    //     // 目前master_next 为true的只有master指令，所以不用比对cfg name
    //     if cfg.master_next {
    //         // cmd: master
    //         packet.set_layer(LayerType::MasterOnly);
    //     } else {
    //         // 非master指令check后处理
    //         match cfg.cmd_type {
    //             // cmd: hashkeyq $key
    //             CommandType::SwallowedCmdHashkeyq => {
    //                 let key = packet.parse_key()?;
    //                 // let hash: i64;
    //                 // 如果key为-1，需要把指令发送到所有分片，但只返回一个分片的响应
    //                 // if key.len() == 2 && key.at(0) == ('-' as u8) && key.at(1) == ('1' as u8) {
    //                 //     log::info!("+++ will broadcast: {:?}", packet.inner_data());
    //                 //     hash = crate::MAX_DIRECT_HASH;
    //                 // } else {
    //                 let hash = calculate_hash(cfg, alg, &key);
    //                 // }

    //                 // 记录reserved hash，为下一个指令使用
    //                 packet.update_reserved_hash(hash);
    //             }
    //             // cmd: hashrandomq
    //             CommandType::SwallowedCmdHashrandomq => {
    //                 // 虽然hash名义为i64，但实际当前均为u32
    //                 let hash = rand::random::<u32>();
    //                 // 记录reserved hash，为下一个指令使用
    //                 packet.update_reserved_hash(hash as i64);
    //             }
    //             _ => {
    //                 debug_assert!(false, "unknown swallowed cmd:{}", cfg.name);
    //                 log::warn!("should not come here![hashkey?]");
    //             }
    //         }
    //     }

    //     // 吞噬掉整个cmd，准备处理下一个cmd fishermen
    //     packet.ignore_all_bulks()?;
    //     // 吞噬/trim掉当前 cmd data，准备解析下一个cmd
    //     packet.trim_swallowed_cmd()?;

    //     Ok(())
    // }

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

        if *oft <= data.len() {
            //let mem = s.take(*oft);
            //let mut flag = Flag::new();
            // TODO 这次需要测试err场景 fishermen
            //flag.set_status_ok(true);
            return Ok(Some(Command::from_ok(s.take(*oft))));
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

    // 构建本地响应resp策略：
    //  1 对于hashkey、keyshard直接构建resp；
    //  2 对于除keyshard外的multi+多bulk req，构建nil rsp；(注意keyshard是mulit+多bulk)
    //  2 对其他固定响应的请求，构建padding rsp；
    // fn build_local_response<F: Fn(i64) -> usize>(
    //     &self,
    //     req: &HashedCommand,
    //     dist_fn: F,
    // ) -> Command {
    //     let cfg = command::get_cfg(req.op_code()).expect(format!("req:{:?}", req).as_str());

    //     // 对hashkey、keyshard构建协议格式的resp，对其他请求构建nil or padding rsp
    //     let rsp = match cfg.name {
    //         command::DIST_CMD_HASHKEY => {
    //             let shard = dist_fn(req.hash());
    //             cfg.build_rsp_hashkey(shard)
    //         }
    //         command::DIST_CMD_KEYSHARD => {
    //             assert!(cfg.multi && cfg.need_bulk_num, "{} cfg malformed", cfg.name);
    //             let shard = dist_fn(req.hash());
    //             cfg.build_rsp_keyshard(shard)
    //         }
    //         _ => {
    //             if cfg.multi && cfg.need_bulk_num {
    //                 cfg.build_nil_rsp()
    //             } else {
    //                 cfg.build_padding_rsp()
    //             }
    //         }
    //     };
    //     rsp
    // }

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
                match cfg.cmd_type {
                    // CommandType::SpecLocalCmdHashkey => {
                    //     // format!(":{}\r\n", shard)
                    //     w.write(b":")?;
                    //     w.write(ctx.request_shard().to_string().as_bytes())?;
                    //     w.write(b"\r\n")?;
                    // }
                    _ => {
                        let padding = cfg.get_padding_rsp();
                        w.write(padding.as_bytes())?;
                    }
                };
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
                    w.write_s_u16(request.key_count())?;
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
                    _ => {
                        let padding = cfg.get_padding_rsp();
                        w.write(padding.as_bytes())?;
                    }
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

    // redis writeback场景：hashkey -1 时，需要对所有节点进行数据（一般为script）分发
    #[inline]
    fn build_writeback_request<C, M, I>(
        &self,
        _ctx: &mut C,
        _response: &Command,
        _: u32,
    ) -> Option<HashedCommand>
    where
        C: Commander<M, I>,
        M: Metric<I>,
        I: MetricItem,
    {
        // let hash_cmd = ctx.request_mut();
        // debug_assert!(hash_cmd.direct_hash(), "data: {:?}", hash_cmd.data());

        // hash idx 放到topo.send 中处理
        // let idx_hash = hash_cmd.hash() - 1;
        // hash_cmd.update_hash(idx_hash);
        // 去掉ignore rsp，改由drop_on_done来处理
        // hash_cmd.set_ignore_rsp(true);
        None
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
