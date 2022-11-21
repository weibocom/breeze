mod command;
mod error;
mod reqpacket;
mod rsppacket;

use crate::msgque::mcq::text::rsppacket::RspPacket;
use crate::{
    Command, Commander, Error, Flag, HashedCommand, Protocol, RequestProcessor, Result, Stream,
    Writer,
};

use sharding::hash::Hash;

use self::reqpacket::RequestPacket;

#[derive(Clone, Default)]
pub struct McqText;

impl McqText {
    #[inline]
    fn parse_request_inner<S: Stream, H: Hash, P: RequestProcessor>(
        &self,
        stream: &mut S,
        _alg: &H,
        process: &mut P,
    ) -> Result<()> {
        log::debug!("+++ rec mcq req:{:?}", stream.slice());

        let mut packet = RequestPacket::new(stream);
        while packet.available() {
            packet.parse_req()?;
            let cfg = packet.cmd_cfg();
            let mut flag = Flag::from_op(cfg.op_code(), cfg.operation().clone());

            // mcq 总是需要重试，确保所有消息读写成功
            flag.set_try_next_type(crate::TryNextType::TryNext);
            // mcq 只需要写一次成功即可，不存在noreply(即只sent) req
            flag.set_sentonly(false);
            // 是否内部请求,不发往后端，如quit
            flag.set_noforward(cfg.noforward());
            let cmd = packet.take();
            let req = HashedCommand::new(cmd, 0, flag);
            process.process(req, true);

            // packet 第一个req处理完毕，准备进行下一次parse
            packet.prepare_for_parse();
        }
        Ok(())
    }

    #[inline]
    fn parse_response_inner<S: Stream>(&self, s: &mut S) -> Result<Option<Command>> {
        // let data = s.slice();
        // assert!(data.len() > 0);
        log::debug!("+++ will parse mcq rsp: {:?}", s.slice());

        let mut packet = RspPacket::new(s);
        packet.parse()?;

        let mut flag = Flag::new();
        if packet.is_succeed() {
            flag.set_status_ok(true);
        }
        let mem = packet.take();
        return Ok(Some(Command::new(flag, mem)));
    }
}

impl Protocol for McqText {
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

    #[inline]
    fn parse_response<S: Stream>(&self, data: &mut S) -> Result<Option<Command>> {
        match self.parse_response_inner(data) {
            Ok(cmd) => Ok(cmd),
            Err(Error::ProtocolIncomplete) => Ok(None),
            e => e,
        }
    }

    // msgque没有multi请求，直接构建padding rsp即可
    fn build_local_response<F: Fn(i64) -> usize>(
        &self,
        req: &HashedCommand,
        _dist_fn: F,
    ) -> Command {
        let cfg = command::get_cfg(req.op_code()).expect(format!("req:{:?}", req).as_str());
        cfg.build_padding_rsp()
    }

    // mc协议比较简单，除了quit直接断连接，其他指令直接发送即可
    #[inline]
    fn write_response<C: Commander, W: Writer>(&self, ctx: &mut C, w: &mut W) -> Result<()> {
        // 对于quit指令，直接返回Err断连
        let cfg = command::get_cfg(ctx.request().op_code())?;
        if cfg.quit {
            return Err(crate::Error::Quit);
        }

        let rsp = ctx.response().data();
        // 虽然quit的响应长度为0，但不排除有其他响应长度为0的场景，还是用quit属性来断连更安全
        if rsp.len() > 0 {
            w.write_slice(rsp, 0)?;
        }
        Ok(())
    }

    // TODO 暂时保留，备查及比对，待上线稳定一段时间后再删除（预计 2022.12.30之后可以） fishermen
    // #[inline]
    // fn write_no_response<W: Writer, F: Fn(i64) -> usize>(
    //     &self,
    //     req: &HashedCommand,
    //     w: &mut W,
    //     _dist_fn: F,
    // ) -> Result<usize> {
    //     // mcq 没有sentonly指令
    //     assert!(!req.sentonly(), "req: {:?}", req.data());
    //     let cfg = command::get_cfg(req.op_code())?;
    //     let rsp = cfg.padding_rsp();

    //     if rsp.len() > 0 {
    //         log::debug!(
    //             "+++ will write padding rsp/{} for req/{}:{:?}",
    //             rsp,
    //             cfg.name,
    //             req.data(),
    //         );
    //         w.write(rsp.as_bytes())?;
    //         Ok(0)
    //     } else {
    //         // 说明请求是quit，直接返回Err断开连接即可
    //         Err(crate::Error::Quit)
    //     }
    // }
}
