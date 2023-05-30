mod error;
pub(crate) mod packet;

use packet::RespStatus::*;
use packet::*;

pub use packet::Binary;

#[derive(Clone, Default)]
pub struct MemcacheBinary;

use crate::{
    Command, Commander, Error, Flag, HashedCommand, Metric, MetricItem, Protocol, RequestProcessor,
    Result, Stream, Writer,
};

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
        debug_assert!(data.len() > 0, "mc req: {:?}", data.slice());
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
            // try_next_type在需要的时候直接通过Request读取即可。
            //flag.set_try_next_type(req.try_next_type());
            flag.set_sentonly(req.sentonly());
            flag.set_noforward(req.noforward());
            let guard = data.take(packet_len);
            let hash = req.hash(alg);
            let cmd = HashedCommand::new(guard, hash, flag);
            debug_assert!(!cmd.quiet_get());
            process.process(cmd, last);
        }
        Ok(())
    }
    #[inline]
    fn parse_response<S: Stream>(&self, data: &mut S) -> Result<Option<Command>> {
        log::debug!("+++ mc will parse rsp: {:?}", data.slice());
        debug_assert!(data.len() > 0, "rsp: {:?}", data.slice());
        let len = data.len();
        if len >= HEADER_LEN {
            let r = data.slice();
            r.check_response()?;
            let pl = r.packet_len();
            debug_assert!(!r.quiet_get(), "rsp: {:?}", r);
            if len >= pl {
                return if !r.is_quiet() {
                    //let mut flag = Flag::from_op(r.op() as u16, r.operation());
                    //flag.set_status_ok(r.status_ok());
                    Ok(Some(Command::from(r.status_ok(), data.take(pl))))
                } else {
                    // response返回quite请求只有一种情况：出错了。
                    // quite请求是异步处理，直接忽略即可。
                    //assert!(!r.status_ok(), "rsp: {:?}", r);
                    //data.ignore(pl);
                    println!("mc response quiete: {:?}", r);
                    Err(Error::ResponseQuiet)
                };
            } else {
                data.reserve(pl - len);
            }
        }
        Ok(None)
    }
    #[inline]
    fn check(&self, req: &HashedCommand, resp: &Command) {
        assert!(
            !resp.is_quiet() && req.op() == resp.op() && req.opaque() == resp.opaque(),
            "{:?} => {:?}",
            req,
            resp,
        );
    }
    // 在parse_request中可能会更新op_code，在write_response时，再更新回来。
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
        // sendonly 直接返回
        if ctx.request().sentonly() {
            assert!(response.is_none(), "req:{:?}", ctx.request());
            return Ok(());
        }
        // 查询请求统计缓存命中率
        if ctx.request().operation().is_query() {
            ctx.metric()
                .cache(response.as_ref().map(|r| r.ok()).unwrap_or_default());
        }

        // 如果原始请求是quite_get请求，并且not found，则不回写。
        let old_op_code = ctx.request().op_code();

        // 如果原始请求是quite_get请求，并且not found，则不回写。
        // if let Some(rsp) = ctx.response_mut() {
        if let Some(rsp) = response {
            assert!(rsp.len() > 0, "empty rsp:{:?}", rsp);

            // 验证Opaque是否相同. 不相同说明数据不一致
            if ctx.request().opaque() != rsp.opaque() {
                ctx.metric().inconsist(1);
            }

            // 如果quite 请求没拿到数据，直接忽略
            //if QUITE_GET_TABLE[old_op_code as usize] == 1 && !rsp.ok() {
            if is_quiet_get(old_op_code as u8) && !rsp.ok() {
                return Ok(());
            }
            log::debug!("+++ will write mc rsp:{:?}", rsp.data());
            //let data = rsp.data_mut();
            rsp.restore_op(old_op_code as u8);
            w.write_slice(rsp, 0)?;

            return Ok(());
        }

        // 先进行metrics统计
        //self.metrics(ctx.request(), None, ctx);

        match old_op_code as u8 {
            // noop: 第一个字节变更为Response，其他的与Request保持一致
            OP_NOOP => {
                w.write_u8(RESPONSE_MAGIC)?;
                w.write_slice(ctx.request(), 1)
            }

            OP_VERSION => w.write(&VERSION_RESPONSE),
            OP_STAT => w.write(&STAT_RESPONSE),
            OP_GETQ | OP_GETKQ => Ok(()),
            OP_SET | OP_DEL | OP_ADD => {
                w.write(&self.build_empty_response(NotStored, ctx.request()))
            }
            OP_GET | OP_GETS => w.write(&self.build_empty_response(NotFound, ctx.request())),
            OP_QUIT | OP_QUITQ => Err(Error::Quit),
            _ => Err(Error::OpCodeNotSupported(old_op_code)),
        }
    }

    // 如果是写请求，把cas请求转换为set请求。
    // 如果是读请求，则通过response重新构建一个新的写请求。
    #[inline]
    fn build_writeback_request<C, M, I>(
        &self,
        ctx: &mut C,
        response: &Command,
        exp_sec: u32,
    ) -> Option<HashedCommand>
    where
        C: Commander<M, I>,
        M: Metric<I>,
        I: MetricItem,
    {
        if ctx.request_mut().operation().is_retrival() {
            let req = &*ctx.request();
            self.build_write_back_get(req, response, exp_sec)
        } else {
            self.build_write_back_inplace(ctx.request_mut());
            None
        }
    }
}
impl MemcacheBinary {
    // 根据req构建response，status为mc协议status，共11种
    #[inline]
    fn build_empty_response(&self, status: RespStatus, req: &HashedCommand) -> [u8; HEADER_LEN] {
        //let req_slice = req.data();
        let mut response = [0; HEADER_LEN];
        response[PacketPos::Magic as usize] = RESPONSE_MAGIC;
        response[PacketPos::Opcode as usize] = req.op();
        response[PacketPos::Status as usize + 1] = status as u8;
        //复制 Opaque
        for i in PacketPos::Opaque as usize..PacketPos::Opaque as usize + 4 {
            response[i] = req.at(i);
        }
        response
    }
    #[inline]
    fn build_write_back_inplace(&self, req: &mut HashedCommand) {
        assert!(req.len() >= HEADER_LEN, "req: {:?}", req);
        // 把cas请求转换成非cas请求: cas值设置为0
        req.clear_cas();
        let op = req.map_op_noreply();
        let cmd = req.operation();
        assert!(req.is_quiet(), "rqe:{:?}", req);
        req.reset_flag(op as u16, cmd);
        // 设置只发送标签，发送完成即请求完成。
        req.set_sentonly(true);
        assert!(req.operation().is_store(), "req: {:?}", req);
        assert!(req.sentonly(), "req: {:?}", req);
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
        let rsp_cmd = resp;
        let r_data = req;
        let key_len = r_data.key_len();
        // 4 为expire flag的长度。
        // 先用rsp的精确长度预分配，避免频繁分配内存
        let req_cmd_len = rsp_cmd.len() + 4 + key_len as usize;
        let mut req_cmd: Vec<u8> = Vec::with_capacity(req_cmd_len);
        use ds::Buffer;

        /*============= 构建request header =============*/
        req_cmd.push(Magic::Request as u8); // magic: [0]
        let op = Opcode::SETQ as u8;
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
}
