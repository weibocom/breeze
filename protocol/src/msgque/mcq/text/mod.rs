mod command;
pub(crate) mod error;
mod reqpacket;
mod rsppacket;

use crate::{
    msgque::mcq::text::rsppacket::RspPacket, Command, Commander, Error, Flag, HashedCommand,
    Metric, MetricItem, Protocol, RequestProcessor, Result, Stream, Writer,
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
            let cmd = packet.mark_flags(cmd, req_type)?;
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

        //let mut flag = Flag::new();
        //if packet.is_succeed() {
        //    flag.set_status_ok(true);
        //}
        let mem = packet.take();
        let _ = packet.delay_metric();
        return Ok(Some(Command::from(packet.is_succeed(), mem)));
    }

    // 协议内部的metric统计，全部放置于此
    #[inline]
    fn metrics<C, M, I>(&self, request: &HashedCommand, response: &Command, metrics: &C)
    where
        C: Commander<M, I>,
        M: Metric<I>,
        I: MetricItem,
    {
        if response.ok() {
            match request.operation() {
                crate::Operation::Get | crate::Operation::Gets | crate::Operation::MGet => {
                    *metrics.metric().get(crate::MetricName::Read) += 1
                }
                crate::Operation::Store => *metrics.metric().get(crate::MetricName::Write) += 1,
                _ => {}
            }
        }
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

    // TODO 测试完毕清理
    // msgque没有multi请求，直接构建padding rsp即可
    // fn build_local_response<F: Fn(i64) -> usize>(
    //     &self,
    //     req: &HashedCommand,
    //     _dist_fn: F,
    // ) -> Command {
    //     let cfg = command::get_cfg(req.op_code()).expect(format!("req:{:?}", req).as_str());
    //     cfg.build_padding_rsp()
    // }

    // mc协议比较简单，除了quit直接断连接，其他指令直接发送即可
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

        // 对于quit指令，直接返回Err断连
        if cfg.quit {
            // mc协议的quit，直接断连接
            return Err(crate::Error::Quit);
        }

        if let Some(rsp) = response {
            // 不再创建local rsp，所有server响应的rsp data长度应该大于0
            debug_assert!(rsp.len() > 0, "req:{:?}, rsp:{:?}", request, rsp);
            w.write_slice(rsp, 0)?;
            self.metrics(request, rsp, ctx);
        } else {
            let padding = cfg.get_padding_rsp();
            w.write(padding.as_bytes())?;
            // TODO 写失败尚没有统计（还没merge进来？），暂时先和dev保持一致 fishermen
        }

        // TODO 测试完毕后清理
        // let rsp = ctx.response().data();
        // // 虽然quit的响应长度为0，但不排除有其他响应长度为0的场景，还是用quit属性来断连更安全
        // if rsp.len() > 0 {
        //     w.write_slice(rsp, 0)?;
        // }

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
