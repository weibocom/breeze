// 解析、构建mcq/mcq3 相关协议

mod error;
mod packet;

use packet::Binary;
use sharding::hash::Hash;

use crate::{
    msgque::mcq::binary::packet::{HEADER_LEN, STAT_RESPONSE, VERSION_RESPONSE},
    Command, Commander, Error, Flag, HashedCommand, Protocol, RequestProcessor, Result, Stream,
};

use self::packet::{PacketPos, RESPONSE_MAGIC};

#[derive(Default, Clone)]
pub struct McqBinary;

impl Protocol for McqBinary {
    // 解析mcq2/mcq3 指令
    fn parse_request<S: Stream, H: Hash, P: RequestProcessor>(
        &self,
        data: &mut S,
        _alg: &H,
        process: &mut P,
    ) -> Result<()> {
        assert!(data.len() > 0, "req: {:?}", data.slice());

        log::debug!("+++ rec req: {:?}", data.slice());
        while data.len() >= HEADER_LEN {
            let req = data.slice();
            req.check_request()?;
            let packet_len = req.packet_len();
            if req.len() < packet_len {
                break;
            }

            let cmd = req.operation();
            // let op_code = req.map_op(); // 把quite get请求，转换成单个的get请求
            let mut flag = Flag::from_op(req.op() as u16, cmd);

            // mcq 总是需要重试，确保所有消息写成功
            // flag.set_try_next_type(req.try_next_type());
            flag.set_try_next_type(crate::TryNextType::TryNext);
            // flag.set_sentonly(req.sentonly());
            // mcq 只需要写一次成功即可，不存在noreply(即只sent) req
            flag.set_sentonly(false);
            // 是否内部请求,不发往后端，如quit
            flag.set_noforward(req.noforward());
            let guard = data.take(packet_len);
            // let hash = req.hash(alg);
            // mcq不需要hash计算
            let hash = 0;
            let cmd = HashedCommand::new(guard, hash, flag);
            // assert!(!cmd.data().quiet_get());
            // mcq 没有multi指令
            process.process(cmd, true);
        }
        Ok(())
    }

    fn parse_response<S: Stream>(&self, data: &mut S) -> Result<Option<Command>> {
        assert!(data.len() > 0, "rsp: {:?}", data.slice());
        let len = data.len();
        if len >= HEADER_LEN {
            let r = data.slice();
            r.check_response()?;
            let pl = r.packet_len();
            // assert!(!r.quiet_get());
            log::debug!("+++ parsed response: {:?}", r);
            if len >= pl {
                let mut flag = Flag::from_op(r.op() as u16, r.operation());
                flag.set_status_ok(r.status_ok());
                return Ok(Some(Command::new(flag, data.take(pl))));
            }
        }
        Ok(None)
    }
    // 返回nil convert的数量
    fn write_response<C: Commander, W: crate::Writer>(
        &self,
        ctx: &mut C,
        w: &mut W,
    ) -> Result<usize> {
        // 如果原始请求是quite_get请求，并且not found，则不回写。
        // let old_op_code = ctx.request().op_code();
        let resp = ctx.response();
        let data = resp.data();
        // data.restore_op(old_op_code as u8);
        w.write_slice(data, 0)?;
        Ok(0)
    }
    #[inline]
    fn write_no_response<W: crate::Writer, F: Fn(i64) -> usize>(
        &self,
        req: &HashedCommand,
        w: &mut W,
        _dist_fn: F,
    ) -> Result<usize> {
        // mcq 没有sentonly指令
        assert!(!req.sentonly(), "req: {:?}", req);
        match req.op_code() as u8 {
            // cmd: version
            0x0b => {
                w.write(&VERSION_RESPONSE)?;
                Ok(0)
            }
            // cmd: stat
            0x10 => {
                w.write(&STAT_RESPONSE)?;
                Ok(0)
            }
            // cmd: quit, quitq
            0x07 | 0x17 => Err(Error::Quit),
            // cmd: set
            0x01 => {
                w.write(&self.build_empty_response(0x5, req.data()))?;
                Ok(0)
            } // set: 0x05 Item Not Stored
            // cmd: get
            0x00 => {
                w.write(&self.build_empty_response(0x1, req.data()))?;
                Ok(0)
            } // get: 0x01 NotFound
            _ => {
                log::warn!("+++ mcq NoResponseFound req: {}/{:?}",  old_op_code, ctx.request());
                return Err(Error::NoResponseFound); },
        }
    }
    #[inline]
    fn check(&self, _req: &HashedCommand, _resp: &Command) -> bool {
        true
    }
    // 构建回写请求。
    // 返回None: 说明req复用，build in place
    // 返回新的request
    fn build_writeback_request<C: Commander>(&self, _ctx: &mut C, _: u32) -> Option<HashedCommand> {
        todo!("not implement");
    }
}

impl McqBinary {
    // 构建一个空rsp
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
}
