mod command;
mod error;
mod reqpacket;
mod rsppacket;

use std::sync::Arc;

use crate::msgque::mcq::text::rsppacket::RspPacket;
use crate::{
    CbMetrics, Command, Commander, Error, Flag, HashedCommand, Metric, Protocol, RequestProcessor,
    Result, Stream, Writer,
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
            let req_type = cfg.req_type().to_owned();
            let cmd = packet.take();
            let cmd = packet.mark_flags(cmd, req_type);
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
        let _ = packet.delay_metric();
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

    // fn crate::Metric::flag(){

    // }
    #[inline]
    fn write_response<C: Commander, W: Writer>(
        &self,
        ctx: &mut C,
        w: &mut W,
        metrics: &mut Arc<CbMetrics>,
    ) -> Result<usize> {
        let rsp = ctx.response();
        let data = rsp.data();
        if ctx.request().operation().is_query() && ctx.response().ok() {
            // m.get_metric(num);
            // let a = metrics::Path::new(vec![Resource::MsgQue.name(), "'"]).num("");
            *metrics.uphit() += 1;
        }
        if ctx.request().operation().is_store() && ctx.response().ok() {
            *metrics.downhit() += 1;
        }
        w.write_slice(data, 0)?;

        Ok(0)
    }

    #[inline]
    fn write_no_response<W: Writer, F: Fn(i64) -> usize>(
        &self,
        req: &HashedCommand,
        w: &mut W,
        _dist_fn: F,
    ) -> Result<usize> {
        // mcq 没有sentonly指令
        assert!(!req.sentonly(), "req: {:?}", req.data());
        let cfg = command::get_cfg(req.op_code())?;
        let rsp = cfg.padding_rsp();

        if rsp.len() > 0 {
            log::debug!(
                "+++ will write padding rsp/{} for req/{}:{:?}",
                rsp,
                cfg.name,
                req.data(),
            );
            w.write(rsp.as_bytes())?;
            Ok(0)
        } else {
            // 说明请求是quit，直接返回Err断开连接即可
            Err(crate::Error::Quit)
        }
    }
}
