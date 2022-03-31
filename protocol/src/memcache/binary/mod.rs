mod packet;
use packet::*;

use crate::{Error, Flag, Result, Stream};

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
        assert!(data.len() > 0);
        while data.len() >= HEADER_LEN {
            let mut req = data.slice();
            req.check_request()?;
            let packet_len = req.packet_len();
            if req.len() < packet_len {
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
        assert!(data.len() > 0);
        let len = data.len();
        if len >= HEADER_LEN {
            let r = data.slice();
            r.check_response()?;
            let pl = r.packet_len();
            assert!(!r.quiet_get());
            if len >= pl {
                if !r.is_quiet() {
                    let mut flag = Flag::from_op(r.op() as u16, r.operation());
                    flag.set_status_ok(r.status_ok());
                    return Ok(Some(Command::new(flag, data.take(pl))));
                } else {
                    // response返回quite请求只有一种情况：出错了。
                    // quite请求是异步处理，直接忽略即可。
                    assert!(!r.status_ok());
                    data.ignore(pl);
                }
            }
        }
        Ok(None)
    }
    #[inline]
    fn check(&self, req: &HashedCommand, resp: &Command) -> bool {
        assert!(
            !resp.data().is_quiet()
                && req.data().op() == resp.data().op()
                && req.data().opaque() == resp.data().opaque()
        );
        true
    }
    // 在parse_request中可能会更新op_code，在write_response时，再更新回来。
    #[inline]
    fn write_response<C: crate::Commander, W: crate::Writer>(
        &self,
        ctx: &mut C,
        w: &mut W,
    ) -> Result<()> {
        // 如果原始请求是quite_get请求，并且not found，则不回写。
        let old_op_code = ctx.request().op_code();
        let resp = ctx.response_mut();
        if QUITE_GET_TABLE[old_op_code as usize] == 1 && !resp.ok() {
            return Ok(());
        }
        let data = resp.data_mut();
        data.restore_op(old_op_code as u8);
        w.write_slice(data, 0)
    }
    // 如果是写请求，把cas请求转换为set请求。
    // 如果是读请求，则通过response重新构建一个新的写请求。
    #[inline]
    fn build_writeback_request<C: crate::Commander>(
        &self,
        ctx: &mut C,
        exp_sec: u32,
    ) -> Option<HashedCommand> {
        if ctx.request_mut().operation().is_retrival() {
            let req = &*ctx.request();
            let resp = ctx.response();
            self.build_write_back_get(req, resp, exp_sec)
        } else {
            self.build_write_back_inplace(ctx.request_mut());
            None
        }
    }
    #[inline]
    fn write_no_response<W: crate::Writer>(&self, req: &HashedCommand, w: &mut W) -> Result<()> {
        if req.sentonly() {
            return Ok(());
        }
        match req.op_code() as u8 {
            OP_CODE_NOOP => {
                w.write_u8(RESPONSE_MAGIC)?; // 第一个字节变更为Response，其他的与Request保持一致
                w.write_slice(req.data(), 1)
            }
            0x0b => w.write(&VERSION_RESPONSE),
            0x10 => w.write(&STAT_RESPONSE),
            0x07 | 0x17 => Err(Error::Quit),
            0x09 | 0x0d => Ok(()), // quite get 请求。什么都不做
            0x01 => w.write(&self.build_empty_response(0x5, req.data())), // set: 0x05 Item Not Stored
            0x00 => w.write(&self.build_empty_response(0x1, req.data())), // get: 0x01 NotFound
            _ => Err(Error::NoResponseFound),
        }
    }
}
impl MemcacheBinary {
    #[inline]
    fn build_empty_response(&self, status: u8, req: &ds::RingSlice) -> [u8; HEADER_LEN] {
        let mut response = [0; HEADER_LEN];
        response[PacketPos::Magic as usize] = RESPONSE_MAGIC;
        response[PacketPos::Opcode as usize] = req.op();
        response[PacketPos::Status as usize + 1] = status;
        //复制 Opaque
        for i in PacketPos::Opaque as usize..PacketPos::Opaque as usize + 4 {
            response[i] = req.at(i);
        }
        response
    }
    #[inline]
    fn build_write_back_inplace(&self, req: &mut HashedCommand) {
        let data = req.data_mut();
        assert!(data.len() >= HEADER_LEN);
        // 把cas请求转换成非cas请求: cas值设置为0
        data.clear_cas();
        let op = data.map_op_noreply();
        let cmd = data.operation();
        assert!(data.is_quiet());
        req.reset_flag(op as u16, cmd);
        // 设置只发送标签，发送完成即请求完成。
        req.set_sentonly(true);
        assert!(req.operation().is_store());
        assert!(req.sentonly());
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
        assert!(resp.ok());
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
        assert!(extra_len == 8);
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
