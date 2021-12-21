mod packet;
use packet::*;

use crate::{Error, Flag, Result, Stream};

#[derive(Clone, Default)]
pub struct MemcacheBinary;

use crate::{Command, HashedCommand, Protocol, RequestProcessor};
use sharding::hash::Hash;
impl Protocol for MemcacheBinary {
    // 解析请求。把所有的multi-get请求转换成单一的n个get请求。
    #[inline(always)]
    fn parse_request<S: Stream, H: Hash, P: RequestProcessor>(
        &self,
        data: &mut S,
        alg: &H,
        process: &mut P,
    ) -> Result<()> {
        debug_assert!(data.len() > 0);
        if data.at(PacketPos::Magic as usize) != REQUEST_MAGIC {
            return Err(Error::RequestProtocolNotValid);
        }
        while data.len() >= HEADER_LEN {
            let req = data.slice();
            let packet_len = req.packet_len();
            if req.len() < packet_len {
                break;
            }
            let op_code = req.op();
            // 非quite get请求就是最后一个请求。
            let last = QUITE_GET_TABLE[op_code as usize] == 0;
            // 把quite get请求，转换成单个的get请求

            let mapped_op_code = OPS_MAPPING_TABLE[op_code as usize];
            if mapped_op_code != op_code {
                let idx = PacketPos::Opcode as usize;
                data.update(idx, mapped_op_code);
            }
            // 存储原始的op_code
            let mut flag = Flag::from_op(op_code, COMMAND_IDX[op_code as usize].into());
            if NOREPLY_MAPPING[req.op() as usize] == req.op() {
                flag.set_sentonly();
            }
            if NO_FORWARD_OPS[op_code as usize] == 1 {
                flag.set_noforward();
            }
            let mut hash = alg.hash(&req.key());
            if hash == 0 {
                debug_assert!(req.operation().is_meta() || req.noop());
                use std::sync::atomic::{AtomicU64, Ordering};
                static RND: AtomicU64 = AtomicU64::new(0);
                hash = RND.fetch_add(1, Ordering::Relaxed);
            }
            let guard = data.take(packet_len);
            let cmd = HashedCommand::new(guard, hash, flag);
            // get请求不能是quiet
            debug_assert!(!(cmd.operation().is_retrival() && cmd.sentonly()));
            process.process(cmd, last);
        }
        Ok(())
    }
    #[inline(always)]
    fn parse_response<S: Stream>(&self, data: &mut S) -> Result<Option<Command>> {
        debug_assert!(data.len() > 0);
        if data.at(PacketPos::Magic as usize) != RESPONSE_MAGIC {
            return Err(Error::ResponseProtocolNotValid);
        }
        let len = data.len();
        if len >= HEADER_LEN {
            let r = data.slice();
            let pl = r.packet_len();
            if len >= pl {
                let mut flag = Flag::from_op(r.op(), r.operation());
                if r.status_ok() {
                    flag.set_status_ok();
                }
                return Ok(Some(Command::new(flag, data.take(pl))));
            }
        }
        Ok(None)
    }
    // 在parse_request中可能会更新op_code，在write_response时，再更新回来。
    #[inline(always)]
    fn write_response<C: crate::Commander, W: crate::ResponseWriter>(
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
        if old_op_code != resp.op_code() {
            // 更新resp opcode
            resp.data_mut().update_opcode(old_op_code);
        }
        let len = resp.len();
        let mut oft = 0;
        while oft < len {
            let data = resp.read(oft);
            w.write(data)?;
            oft += data.len();
        }
        Ok(())
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
    #[inline(always)]
    fn write_no_response<W: crate::ResponseWriter>(
        &self,
        req: &HashedCommand,
        w: &mut W,
    ) -> Result<()> {
        match req.op_code() {
            OP_CODE_NOOP => w.write(&NOOP_RESPONSE),
            0xb => w.write(&VERSION_RESPONSE),
            0x10 => w.write(&STAT_RESPONSE),
            0x07 | 0x17 => Err(Error::Quit),
            _ => Err(Error::NoResponseFound),
        }
    }
}
impl MemcacheBinary {
    #[inline(always)]
    fn build_write_back_inplace(&self, req: &mut HashedCommand) {
        let data = req.data_mut();
        debug_assert!(data.len() >= HEADER_LEN);
        let op_code = NOREPLY_MAPPING[data.op() as usize];
        // 把cop_code替换成quite command.
        data.update_opcode(op_code);
        // 把cas请求转换成非cas请求: cas值设置为0
        data.clear_cas();
        // 更新flag
        let op = COMMAND_IDX[op_code as usize].into();
        req.reset_flag(op_code, op);
        // 设置只发送标签，发送完成即请求完成。
        req.set_sentonly();
        debug_assert!(req.operation().is_store());
        debug_assert!(req.sentonly());
    }
    #[inline(always)]
    fn build_write_back_get(
        &self,
        req: &HashedCommand,
        resp: &Command,
        exp_sec: u32,
    ) -> Option<HashedCommand> {
        // 轮询response的cmds，构建回写request
        // 只为status为ok的resp构建回种req
        debug_assert!(resp.ok());
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
        debug_assert!(extra_len == 8);
        req_cmd.push(extra_len); // extra len: [4]
        req_cmd.push(0 as u8); // data type，全部为0: [5]
        req_cmd.write_u16(0 as u16); // vbucket id, 回写全部为0,pos [6,7]
        let total_body_len = extra_len as u32 + key_len as u32 + rsp_cmd.value_len();
        req_cmd.write_u32(total_body_len); // total body len [8-11]
        req_cmd.write_u32(0 as u32); // opaque: [12, 15]
        req_cmd.write_u64(0 as u64); // cas: [16, 23]

        /*============= 构建request body =============*/
        rsp_cmd.extra_or_flag().copy_to_vec(&mut req_cmd); // extra之flag: [24, 27]
        req_cmd.write_u32(exp_sec); // extra之expiration：[28,31]
        r_data.key().copy_to_vec(&mut req_cmd);
        rsp_cmd.value().copy_to_vec(&mut req_cmd);

        let hash = req.hash();
        let mut flag = Flag::from_op(op, COMMAND_IDX[op as usize].into());
        flag.set_sentonly();

        let guard = ds::MemGuard::from_vec(req_cmd);
        Some(HashedCommand::new(guard, hash, flag))
    }
}
