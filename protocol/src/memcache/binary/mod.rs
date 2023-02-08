mod error;
mod packet;

use packet::*;

use crate::{Error, Flag, MetricName, Result, Stream};

#[derive(Clone, Default)]
pub struct MemcacheBinary;

use crate::{Command, HashedCommand, Protocol, RequestProcessor};
use sharding::hash::Hash;
impl Protocol for MemcacheBinary {
    // 解析请求。把所有的multi-get请求转换成单一的n个get请求。
    #[inline]
    fn parse_request<S: Stream, H: Hash, P: RequestProcessor>(
        &self,
        data: &mut S,
        alg: &H,
        process: &mut P,
    ) -> Result<()> {
        log::debug!("+++ recv mc:{:?}", data.slice());
        assert!(data.len() > 0, "mc req: {:?}", data.slice());
        while data.len() >= HEADER_LEN {
            let mut req = data.slice();
            req.check_request()?;
            let packet_len = req.packet_len();
            if req.len() < packet_len {
                data.reserve(packet_len - req.len());
                break;
            }

            let last = !req.quiet_get(); // 须在map_op之前获取
            let cmd = req.operation();
            let op_code = req.map_op(); // 把quite get请求，转换成单个的get请求
            let mut flag = Flag::from_op(op_code as u16, cmd);
            flag.set_try_next_type(req.try_next_type());
            flag.set_sentonly(req.sentonly());
            flag.set_noforward(req.noforward());
            let guard = data.take(packet_len);
            let hash = req.hash(alg);
            let cmd = HashedCommand::new(guard, hash, flag);
            assert!(!cmd.data().quiet_get());
            process.process(cmd, last);
        }
        Ok(())
    }
    #[inline]
    fn parse_response<S: Stream>(&self, data: &mut S) -> Result<Option<Command>> {
        log::debug!("+++ mc will parse rsp: {:?}", data.slice());
        assert!(data.len() > 0, "rsp: {:?}", data.slice());
        let len = data.len();
        if len >= HEADER_LEN {
            let r = data.slice();
            r.check_response()?;
            let pl = r.packet_len();
            assert!(!r.quiet_get(), "rsp: {:?}", r);
            if len >= pl {
                if !r.is_quiet() {
                    let mut flag = Flag::from_op(r.op() as u16, r.operation());
                    flag.set_status_ok(r.status_ok());
                    return Ok(Some(Command::new(flag, data.take(pl))));
                } else {
                    // response返回quite请求只有一种情况：出错了。
                    // quite请求是异步处理，直接忽略即可。
                    assert!(!r.status_ok(), "rsp: {:?}", r);
                    data.ignore(pl);
                }
            } else {
                data.reserve(pl - len);
            }
        }
        Ok(None)
    }
    #[inline]
    fn check(&self, req: &HashedCommand, resp: &Command) {
        assert!(
            !resp.data().is_quiet()
                && req.data().op() == resp.data().op()
                && req.data().opaque() == resp.data().opaque(),
            "{:?} => {:?}",
            req,
            resp,
        );
    }
    // 在parse_request中可能会更新op_code，在write_response时，再更新回来。
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
        // sendonly 直接返回
        if ctx.request().sentonly() {
            assert!(response.is_none(), "req:{:?}", ctx.request());
            return Ok(());
        }

        // 如果原始请求是quite_get请求，并且not found，则不回写。
        let old_op_code = ctx.request().op_code();

        // 如果原始请求是quite_get请求，并且not found，则不回写。
        // if let Some(rsp) = ctx.response_mut() {
        if let Some(rsp) = response {
            assert!(rsp.data().len() > 0, "empty rsp:{:?}", rsp);

            // 先进行metrics统计
            self.metrics(ctx.request(), Some(&rsp), ctx);

            // 如果quite 请求没拿到数据，直接忽略
            if QUITE_GET_TABLE[old_op_code as usize] == 1 && !rsp.ok() {
                return Ok(());
            }
            log::debug!("+++ will write mc rsp:{:?}", rsp.data());
            let data = rsp.data_mut();
            data.restore_op(old_op_code as u8);
            w.write_slice(data, 0)?;

            return Ok(());
        }

        // 先进行metrics统计
        self.metrics(ctx.request(), None, ctx);

        match old_op_code as u8 {
            // noop: 第一个字节变更为Response，其他的与Request保持一致
            OP_CODE_NOOP => {
                w.write_u8(RESPONSE_MAGIC)?;
                w.write_slice(ctx.request().data(), 1)?;
            }

            //version: 返回固定rsp
            OP_CODE_VERSION => w.write(&VERSION_RESPONSE)?,

            // stat：返回固定rsp
            OP_CODE_STAT => w.write(&STAT_RESPONSE)?,

            // quit/quitq 无需返回rsp
            OP_CODE_QUIT | OP_CODE_QUITQ => return Err(Error::Quit),

            // quite get 请求，无需返回任何rsp，但没实际发送，rsp_ok设为false
            OP_CODE_GETQ | OP_CODE_GETKQ => return Ok(()),
            // 0x09 | 0x0d => return Ok(()),

            // set: mc status设为 Item Not Stored,status设为false
            OP_CODE_SET => {
                w.write(&self.build_empty_response(RespStatus::NotStored, ctx.request()))?
            }
            // self.build_empty_response(RespStatus::NotStored, req)

            // get，返回key not found 对应的0x1
            OP_CODE_GET | OP_CODE_GETS => {
                w.write(&self.build_empty_response(RespStatus::NotFound, ctx.request()))?
            }

            // self.build_empty_response(RespStatus::NotFound, req)

            // TODO：之前是直接mesh断连接，现在返回异常rsp，由client决定应对，观察副作用 fishermen
            _ => return Err(Error::NoResponseFound),
        }
        Ok(())
    }

    // 如果是写请求，把cas请求转换为set请求。
    // 如果是读请求，则通过response重新构建一个新的写请求。
    #[inline]
    fn build_writeback_request<C: crate::Commander>(
        &self,
        ctx: &mut C,
        response: &Command,
        exp_sec: u32,
    ) -> Option<HashedCommand> {
        if ctx.request_mut().operation().is_retrival() {
            let req = &*ctx.request();
            self.build_write_back_get(req, response, exp_sec)
        } else {
            self.build_write_back_inplace(ctx.request_mut());
            None
        }
    }

    // 构建本地响应resp策略：
    //  1 对于hashkey、keyshard直接构建resp；
    //  2 对于除keyshard外的multi+ need bulk num 的 req，构建nil rsp；(注意keyshard是mulit+多bulk)
    //  2 对其他固定响应的请求，构建padding rsp；
    // fn build_local_response<F: Fn(i64) -> usize>(
    //     &self,
    //     req: &HashedCommand,
    //     _dist_fn: F,
    // ) -> Command {
    //     if req.sentonly() {
    //         let mut rsp = Command::from_vec(Vec::with_capacity(0));
    //         rsp.set_status_ok(true);
    //         return rsp;
    //     }

    //     let mut resp_ok = true;
    //     let resp_data = match req.op_code() as u8 {
    //         // noop: 第一个字节变更为Response，其他的与Request保持一致
    //         OP_CODE_NOOP => {
    //             let mut rsp_noop = Vec::with_capacity(req.data().len());
    //             req.data().copy_to_vec(&mut rsp_noop);
    //             rsp_noop[0] = RESPONSE_MAGIC;
    //             rsp_noop
    //             // w.write_u8(RESPONSE_MAGIC)?;
    //             // w.write_slice(req.data(), 1)?;
    //             // Ok(0)
    //         }

    //         //version: 返回固定rsp
    //         OP_CODE_VERSION => Vec::from(VERSION_RESPONSE),
    //         // w.write(&VERSION_RESPONSE)?;
    //         // Ok(0)

    //         // stat：返回固定rsp
    //         OP_CODE_STAT => Vec::from(STAT_RESPONSE),
    //         // w.write(&STAT_RESPONSE)?;
    //         // Ok(0)

    //         // quit/quitq 无需返回rsp
    //         OP_CODE_QUIT | OP_CODE_QUITQ => Vec::with_capacity(0),

    //         // quite get 请求，无需返回任何rsp，但没实际发送，rsp_ok设为false
    //         0x09 | 0x0d => {
    //             resp_ok = false;
    //             Vec::with_capacity(0) // Ok(0),
    //         }
    //         // set: mc status设为 Item Not Stored,status设为false
    //         0x01 => {
    //             resp_ok = false;
    //             self.build_empty_response(RespStatus::NotStored, req)
    //             // w.write(&self.build_empty_response(0x5, req.data()))?;
    //             // Ok(0)
    //         }

    //         // get，返回key not found 对应的0x1
    //         0x00 => {
    //             resp_ok = false;
    //             self.build_empty_response(RespStatus::NotFound, req)
    //             // w.write(&self.build_empty_response(0x1, req.data()))?;
    //             // Ok(0)
    //         }

    //         // TODO：之前是直接mesh断连接，现在返回异常rsp，由client决定应对，观察副作用 fishermen
    //         _ => {
    //             resp_ok = false;
    //             log::warn!("+++ found unsupported local rsp for mc req:{:?}", req);
    //             self.build_empty_response(RespStatus::InvalidArg, req)
    //         }
    //     };
    //     let mut req = Command::from_vec(resp_data);
    //     req.set_status_ok(resp_ok);
    //     req
    // }

    // TODO 暂时保留，备查及比对，待上线稳定一段时间后再删除（预计 2022.12.30之后可以） fishermen
    // #[inline]
    // fn write_no_response<W: crate::Writer, F: Fn(i64) -> usize>(
    //     &self,
    //     req: &HashedCommand,
    //     w: &mut W,
    //     _dist_fn: F,
    // ) -> Result<usize> {
    //     if req.sentonly() {
    //         return Ok(0);
    //     }
    //     match req.op_code() as u8 {
    //         OP_CODE_NOOP => {
    //             w.write_u8(RESPONSE_MAGIC)?; // 第一个字节变更为Response，其他的与Request保持一致
    //             w.write_slice(req.data(), 1)?;
    //             Ok(0)
    //         }
    //         0x0b => {
    //             w.write(&VERSION_RESPONSE)?;
    //             Ok(0)
    //         }
    //         0x10 => {
    //             w.write(&STAT_RESPONSE)?;
    //             Ok(0)
    //         }
    //         0x07 | 0x17 => Err(Error::Quit),
    //         0x09 | 0x0d => Ok(0),
    //         // quite get 请求。什么都不做
    //         0x01 => {
    //             w.write(&self.build_empty_response(0x5, req.data()))?;
    //             Ok(0)
    //         }
    //         // set: 0x05 Item Not Stored
    //         0x00 => {
    //             w.write(&self.build_empty_response(0x1, req.data()))?;
    //             Ok(0)
    //         }
    //         // get: 0x01 NotFound
    //         _ => Err(Error::NoResponseFound),
    //     }
    // }
}
impl MemcacheBinary {
    // 根据req构建response，status为mc协议status，共11种
    #[inline]
    fn build_empty_response(&self, status: RespStatus, req: &HashedCommand) -> [u8; HEADER_LEN] {
        let req_slice = req.data();
        let mut response = [0; HEADER_LEN];
        response[PacketPos::Magic as usize] = RESPONSE_MAGIC;
        response[PacketPos::Opcode as usize] = req_slice.op();
        response[PacketPos::Status as usize + 1] = status as u8;
        //复制 Opaque
        for i in PacketPos::Opaque as usize..PacketPos::Opaque as usize + 4 {
            response[i] = req_slice.at(i);
        }
        response
    }
    #[inline]
    fn build_write_back_inplace(&self, req: &mut HashedCommand) {
        let data = req.data_mut();
        assert!(data.len() >= HEADER_LEN, "req: {:?}", data);
        // 把cas请求转换成非cas请求: cas值设置为0
        data.clear_cas();
        let op = data.map_op_noreply();
        let cmd = data.operation();
        assert!(data.is_quiet(), "rqe:{:?}", data);
        req.reset_flag(op as u16, cmd);
        // 设置只发送标签，发送完成即请求完成。
        req.set_sentonly(true);
        assert!(req.operation().is_store(), "req: {:?}", req.data());
        assert!(req.sentonly(), "req: {:?}", req.data());
    }
    #[inline]
    fn build_write_back_get(
        &self,
        req: &HashedCommand,
        resp: &Command,
        exp_sec: u32,
    ) -> Option<HashedCommand> {
        // 轮询response的cmds，构建回写request
        // 只为status为ok的resp构建回种req
        assert!(resp.ok(), "resp: {:?}", resp.data());
        let rsp_cmd = resp.data();
        let r_data = req.data();
        let key_len = r_data.key_len();
        // 4 为expire flag的长度。
        // 先用rsp的精确长度预分配，避免频繁分配内存
        let req_cmd_len = rsp_cmd.len() + 4 + key_len as usize;
        let mut req_cmd: Vec<u8> = Vec::with_capacity(req_cmd_len);
        use ds::Buffer;

        /*============= 构建request header =============*/
        req_cmd.push(Magic::Request as u8); // magic: [0]
        let op = Opcode::SetQ as u8;
        req_cmd.push(op); // opcode: [1]
        req_cmd.write_u16(key_len); // key len: [2,3]
        let extra_len = rsp_cmd.extra_len() + 4 as u8; // get response中的extra 应该是4字节，作为set的 flag，另外4字节是set的expire
        assert!(extra_len == 8, "extra_len: {}", extra_len);
        req_cmd.push(extra_len); // extra len: [4]
        req_cmd.push(0 as u8); // data type，全部为0: [5]
        req_cmd.write_u16(0 as u16); // vbucket id, 回写全部为0,pos [6,7]
        let total_body_len = extra_len as u32 + key_len as u32 + rsp_cmd.value_len();
        req_cmd.write_u32(total_body_len); // total body len [8-11]
        req_cmd.write_u32(u32::MAX); // opaque: [12, 15]
        req_cmd.write_u64(0 as u64); // cas: [16, 23]

        /*============= 构建request body =============*/
        rsp_cmd.extra_or_flag().copy_to_vec(&mut req_cmd); // extra之flag: [24, 27]
        req_cmd.write_u32(exp_sec); // extra之expiration：[28,31]
        r_data.key().copy_to_vec(&mut req_cmd);
        rsp_cmd.value().copy_to_vec(&mut req_cmd);

        let hash = req.hash();
        let mut flag = Flag::from_op(op as u16, COMMAND_IDX[op as usize].into());
        flag.set_sentonly(true);

        let guard = ds::MemGuard::from_vec(req_cmd);
        // TODO: 目前mc不需要用key_count，等又需要再调整
        Some(HashedCommand::new(guard, hash, flag))
    }

    // 当前只统计缓存命中率，后续需要其他统计，在此增加
    #[inline(always)]
    fn metrics<M: crate::Metric<T>, T: std::ops::AddAssign<i64> + std::ops::AddAssign<bool>>(
        &self,
        request: &HashedCommand,
        response: Option<&Command>,
        metrics: &M,
    ) {
        if request.operation().is_query() {
            *metrics.get(MetricName::Cache) += response.map(|rsp| rsp.ok()).unwrap_or_default();
        }
    }
}
