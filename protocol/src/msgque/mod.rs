use crate::{
    Command, Commander, Error, Flag, HashedCommand, Metric, MetricItem, Operation::*, Protocol,
    RequestProcessor, Result, Stream, Writer,
};
use ds::ByteOrder;
use sharding::hash::Hash;

pub const OP_GET: u16 = 0;
pub const OP_SET: u16 = 1;
pub const OP_STATS: u16 = 2;
pub const OP_VERSION: u16 = 3;
pub const OP_QUIT: u16 = 4;

pub type MsgQue = McqText;

#[derive(Clone, Default)]
pub struct McqText;

impl Protocol for McqText {
    #[inline]
    fn parse_request<S: Stream, H: Hash, P: RequestProcessor>(
        &self,
        stream: &mut S,
        _alg: &H,
        process: &mut P,
    ) -> Result<()> {
        let data = stream.slice();
        let mut oft = 0;
        while let Some(mut lfcr) = data.find_lf_cr(oft) {
            let head4 = data.u32_le(oft);
            let (op_code, op) = if head4 == u32::from_le_bytes(*b"get ") {
                (OP_GET, Get)
            } else if head4 == u32::from_le_bytes(*b"set ") {
                // <command name> <key> <flags> <exptime> <bytes>\r\n
                let line_oft = lfcr + 2;
                // 命令之后第四个空格是数据长度
                let Some(space) = data.find_r_n(oft + 4..line_oft, b' ', 3) else {
                    return Err(Error::ProtocolNotSupported);
                };
                let Some(val_len) = data.try_str_num(space + 1..lfcr) else {
                    return Err(Error::ProtocolNotSupported);
                };
                // 大value一次申请
                lfcr = line_oft + val_len;
                if data.len() < lfcr + 2 {
                    stream.reserve(lfcr + 2 - data.len());
                    return Ok(());
                }
                if data[lfcr] != b'\r' || data[lfcr + 1] != b'\n' {
                    return Err(Error::ProtocolNotSupported);
                }
                (OP_SET, Store)
            } else if head4 == u32::from_le_bytes(*b"stat") {
                //stats
                (OP_STATS, Meta)
            } else if head4 == u32::from_le_bytes(*b"vers") {
                //version
                (OP_VERSION, Meta)
            } else if head4 == u32::from_le_bytes(*b"quit") {
                (OP_QUIT, Meta)
            } else {
                return Err(Error::ProtocolNotSupported);
            };

            let cmd = stream.take(lfcr + 2 - oft);
            oft = lfcr + 2;
            let mut flag = Flag::from_op(op_code, op);
            flag.set_noforward(op == Meta);
            let req = HashedCommand::new(cmd, 0, flag);
            process.process(req, true);
        }
        Ok(())
    }

    #[inline]
    fn parse_response<S: Stream>(&self, stream: &mut S) -> Result<Option<Command>> {
        let data = stream.slice();
        let Some(mut lfcr) = data.find_lf_cr(0) else {
            return Ok(None);
        };
        //最短响应是end\r\n
        if lfcr < 3 {
            return Err(crate::Error::UnexpectedData);
        }
        let head4 = data.u32_le(0);
        let ok = if head4 == u32::from_le_bytes(*b"END\r") {
            false
        // VALUE <key> <flags> <bytes> [<cas unique>]\r\n
        } else if head4 == u32::from_le_bytes(*b"VALU") {
            let line_oft = lfcr + 2;
            let Some(space) = data.find_r_n(4..line_oft, b' ', 3) else {
                return Err(Error::UnexpectedData);
            };
            let Some(val_len) = data.try_str_num(space + 1..lfcr) else {
                return Err(Error::UnexpectedData);
            };
            lfcr = line_oft + val_len;
            //数据之后是\r\nend\r\n
            if data.len() < lfcr + 2 + 5 {
                stream.reserve(lfcr + 2 + 5 - data.len());
                return Ok(None);
            }
            if !data.start_with(lfcr, b"\r\nEND\r\n") {
                return Err(Error::UnexpectedData);
            }
            lfcr = lfcr + 5;
            true
        } else if head4 == u32::from_le_bytes(*b"STOR") {
            //STORED
            true
        } else {
            false
        };
        return Ok(Some(Command::from(ok, stream.take(lfcr + 2))));
    }

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
        let op_code = request.op_code();
        match op_code {
            OP_QUIT => {
                return Err(crate::Error::Quit);
            }
            OP_STATS => {
                w.write(b"STAT supported later\r\nEND\r\n")?;
            }
            OP_VERSION => {
                w.write(b"VERSION 0.0.1\r\n")?;
            }
            _ => {
                if let Some(rsp) = response {
                    w.write_slice(rsp, 0)?;
                    self.metrics(request, rsp, ctx);
                } else {
                    //协议要求服务端错误断连
                    w.write(b"SERVER_ERROR mcq not available\r\n")?;
                    return Err(Error::Quit);
                }
            }
        }
        Ok(())
    }

    fn on_sent(&self, req_op: crate::Operation, metrics: &mut crate::HostMetric) {
        match req_op {
            crate::Operation::Get => metrics.get += 1,
            crate::Operation::Store => metrics.store += 1,
            _ => metrics.others += 1,
        }
    }

    /// 最大重试次数，mq的写设置为50，即认为必须成功
    #[inline]
    fn max_tries(&self, req_op: crate::Operation) -> u8 {
        match req_op {
            crate::Operation::Store => 10,
            _ => 1,
        }
    }
}

impl McqText {
    // 响应发送时，统计请求最终成功的qps
    #[inline]
    fn metrics<C, M, I>(&self, request: &HashedCommand, response: &Command, metrics: &C)
    where
        C: Commander<M, I>,
        M: Metric<I>,
        I: MetricItem,
    {
        if response.ok() {
            match request.operation() {
                crate::Operation::Get => *metrics.metric().get(crate::MetricName::Read) += 1,
                crate::Operation::Store => *metrics.metric().get(crate::MetricName::Write) += 1,
                _ => {}
            }
        }
    }
}
