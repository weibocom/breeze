// phantom 也走redis协议，只是key格式不同，请求的key格式为hashkey.realkey，解析时，需要进行协议转换
// 为了避免对redis的侵入，独立实现

mod command;
mod error;
mod flag;
mod packet;
//mod token;

use crate::{
    Command, Commander, Error, Flag, HashedCommand, MetricName, Protocol, RequestProcessor, Result,
    Stream,
};
use ds::RingSlice;
use flag::RedisFlager;
use packet::Packet;
use sharding::hash::Hash;

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
        log::debug!("+++ rec req:{:?}", stream.slice());

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
                    assert!(cfg.has_key, "cmd: {:?}", cfg.name);
                    // 注意，fullkey格式: $hash_key.$realkey
                    let full_key = packet.parse_key()?;
                    let (hash, real_key) = split_and_calculate_hash(alg, &full_key);
                    debug_assert!(!cfg.has_val, "cmd:{}", cfg.name);
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
                    log::info!("unsupport rsp:{:?}, pos: {}/{}", data, oft, bulk_count);
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
        log::debug!("+++ will parse rsp:{:?}", data);

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
                    log::info!("phantom not supported:{:?}", data);
                    panic!("not supported");
                }
            }
            assert!(oft <= data.len(), "oft:{}/{:?}", oft, data);
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
            e => {
                log::warn!(
                    "+++ phantom parsed err: {:?}, req: {:?} ",
                    e,
                    stream.slice()
                );
                e
            }
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

    //TODO 测试完毕后清理
    // 构建本地响应resp策略：
    //  1 对于multi+多bulk req，构建nil rsp；
    //  2 对其他固定响应的请求，构建padding rsp；
    // fn build_local_response<F: Fn(i64) -> usize>(
    //     &self,
    //     req: &HashedCommand,
    //     _dist_fn: F,
    // ) -> Command {
    //     let cfg = command::get_cfg(req.op_code()).expect(format!("req:{:?}", req).as_str());

    //     // 对hashkey、keyshard构建协议格式的resp，对其他请求构建nil or padding rsp
    //     if cfg.multi && cfg.need_bulk_num {
    //         cfg.build_nil_rsp()
    //     } else {
    //         cfg.build_padding_rsp()
    //     }
    // }

    // 发送响应给client：
    //  1 multi + need-bulk-num，对first先发bulk num，非first正常发送；
    //  2 其他 multi，只发first响应，其他忽略；
    //  3 quit 发送完毕后，返回Err 断开连接
    //  4 其他普通响应直接发送；
    #[inline]
    fn write_response<
        C: crate::Commander + crate::Metric<T>,
        W: crate::Writer,
        T: std::ops::AddAssign<i64> + std::ops::AddAssign<bool>,
    >(
        &self,
        ctx: &mut C,
        response: Option<&mut Command>,
        w: &mut W,
    ) -> Result<()> {
        let request = ctx.request();
        let cfg = command::get_cfg(request.op_code())?;

        if !cfg.multi {
            if let Some(rsp) = response {
                w.write_slice(rsp.data(), 0)?;
            } else {
                let padding = cfg.get_padding_rsp();
                w.write(padding.as_bytes())?;
            }

            // quit 指令发送响应后，返回Err关闭连接
            if cfg.quit {
                return Err(crate::Error::Quit);
            }
        } else {
            let ext = request.ext();
            let first = ext.mkey_first();
            if first || cfg.need_bulk_num {
                if first && cfg.need_bulk_num {
                    w.write_u8(b'*')?;
                    w.write(ext.key_count().to_string().as_bytes())?;
                    w.write(b"\r\n")?;
                }

                // 如果rsp是ok，或者不需要bulk num，直接发送；否则构建rsp or padding rsp
                if let Some(rsp) = response {
                    if rsp.ok() || !cfg.need_bulk_num {
                        w.write_slice(rsp.data(), 0)?;
                        return Ok(());
                    }
                }

                let padding = cfg.get_padding_rsp();
                w.write(padding.as_bytes())?;

                // rsp不为ok，对need_bulk_num为true的cmd进行nil convert 统计
                if cfg.need_bulk_num {
                    *ctx.get(MetricName::NilConvert) += 1;
                }
            }
            // 有些请求，如mset，不需要bulk_num,说明只需要返回一个首个key的请求即可。
            // mset always return +OK
            // https://redis.io/commands/mset
        }
        Ok(())
    }

    // phantom writeback场景：所有的bfset or bfmset
    #[inline]
    fn build_writeback_request<C: Commander>(
        &self,
        _ctx: &mut C,
        _response: &Command,
        _: u32,
    ) -> Option<HashedCommand> {
        // 目前不需要对req做调整
        None
    }

    // TODO 暂时保留，备查及比对，待上线稳定一段时间后再删除（预计 2022.12.30之后可以） fishermen
    // #[inline]
    // fn write_no_response<W: crate::Writer, F: Fn(i64) -> usize>(
    //     &self,
    //     req: &HashedCommand,
    //     w: &mut W,
    //     _dist_fn: F,
    // ) -> Result<usize> {
    //     let cfg = command::get_cfg(req.op_code())?;
    //     let mut nil_rsp = false;
    //     if cfg.multi {
    //         let ext = req.ext();
    //         let first = ext.mkey_first();
    //         if first || cfg.need_bulk_num {
    //             if first && cfg.need_bulk_num {
    //                 w.write_u8(b'*')?;
    //                 w.write(ext.key_count().to_string().as_bytes())?;
    //                 w.write(b"\r\n")?;
    //             }

    //             // 对于每个key均需要响应，且响应是异常的场景，返回nil，否则继续返回原响应
    //             if cfg.need_bulk_num && cfg.nil_rsp > 0 {
    //                 nil_rsp = true;
    //             }
    //         }
    //     }

    //     let mut nil_convert = 0;
    //     let rsp = if nil_rsp {
    //         let nil = *PADDING_RSP_TABLE.get(cfg.nil_rsp as usize).unwrap();
    //         // 百分之一的概率打印nil 转换
    //         let rd = rand::random::<usize>() % 100;
    //         if log::log_enabled!(log::Level::Debug) || rd == 0 {
    //             log::info!("+++ write pt client nil/{} noforward:{:?}", nil, req.data());
    //         }
    //         nil_convert = 1;
    //         nil.to_string()
    //     } else {
    //         let rsp_idx = req.ext().padding_rsp() as usize;
    //         assert!(rsp_idx < PADDING_RSP_TABLE.len(), "rsp_idx:{}", rsp_idx);
    //         let rsp = *PADDING_RSP_TABLE.get(rsp_idx).unwrap();
    //         rsp.to_string()
    //     };

    //     // TODO 先保留到2022.12，用于快速定位协议问题 fishermen
    //     log::debug!("+++ will write no rsp. req:{}", req);
    //     if rsp.len() > 0 {
    //         w.write(rsp.as_bytes())?;
    //         Ok(nil_convert)
    //     } else {
    //         // quit，先发+OK，再返回err
    //         // assert_eq!(rsp_idx, 0);
    //         let ok_rs = PADDING_RSP_TABLE.get(1).unwrap().as_bytes();
    //         w.write(ok_rs)?;
    //         Err(crate::Error::Quit)
    //     }
    // }
}

use std::sync::atomic::{AtomicI64, Ordering};
static AUTO: AtomicI64 = AtomicI64::new(0);
// 避免异常情况下hash为0，请求集中到某一个shard上。
// hash正常情况下可能为0
#[inline]
fn split_and_calculate_hash<H: Hash>(alg: &H, full_key: &RingSlice) -> (i64, RingSlice) {
    if let Some(idx) = full_key.find(0, b'.') {
        debug_assert!(idx > 0);
        let hash_key = full_key.sub_slice(0, idx);
        let real_key_len = full_key.len() - idx - 1;
        let real_key = full_key.sub_slice(idx + 1, real_key_len);

        debug_assert!(real_key.len() > 0);
        let hash = alg.hash(&hash_key);
        return (hash, real_key);
    }

    log::warn!("phantom - malform key: {:?}", full_key);
    (0, full_key.sub_slice(0, full_key.len()))
}

#[inline]
fn default_hash() -> i64 {
    AUTO.fetch_add(1, Ordering::Relaxed)
}
