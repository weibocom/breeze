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

const GETBYTE: u32 = u32::from_le_bytes(*b"get ");
const SETBYTE: u32 = u32::from_le_bytes(*b"set ");
const STATSBYTE: u32 = u32::from_le_bytes(*b"stat");
const VERSIONBYTE: u32 = u32::from_le_bytes(*b"vers");
const QUITBYTE: u32 = u32::from_le_bytes(*b"quit");

const END: u32 = u32::from_le_bytes(*b"END\r");
const VALUE: u32 = u32::from_le_bytes(*b"VALU");
const STORED: u32 = u32::from_le_bytes(*b"STOR");

#[derive(Clone, Default)]
pub struct MsgQue;

impl Protocol for MsgQue {
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
            let (op_code, op) = match head4 {
                GETBYTE => (OP_GET, Get),
                SETBYTE => {
                    // <command name> <key> <flags> <exptime> <bytes>\r\n
                    // 最后一个空格后是数据长度
                    let val_len = Self::val_len(data, oft + 4, lfcr)?;
                    // 大value一次申请
                    lfcr = lfcr + 2 + val_len;
                    if data.len() < lfcr + 2 {
                        stream.reserve(lfcr + 2 - data.len());
                        return Ok(());
                    }
                    if data[lfcr] != b'\r' || data[lfcr + 1] != b'\n' {
                        return Err(Error::ProtocolNotSupported);
                    }
                    (OP_SET, Store)
                }
                STATSBYTE => (OP_STATS, Meta),
                VERSIONBYTE => (OP_VERSION, Meta),
                QUITBYTE => (OP_QUIT, Meta),
                _ => return Err(Error::ProtocolNotSupported),
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
        let ok = match head4 {
            END => false,
            VALUE => {
                // VALUE <key> <flags> <bytes> [<cas unique>]\r\n
                let val_len = Self::val_len(data, 4, lfcr)?;
                let line_oft = lfcr + 2;
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
            }
            STORED => true,
            _ => return Err(Error::UnexpectedData),
        };
        return Ok(Some(Command::from(ok, stream.take(lfcr + 2))));
    }

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
            OP_QUIT => Err(crate::Error::Quit),
            OP_STATS => w.write(b"STAT supported later\r\nEND\r\n"),
            OP_VERSION => w.write(b"VERSION 0.0.1\r\n"),
            _ => {
                if let Some(rsp) = response {
                    w.write_slice(rsp, 0)?;
                    self.metrics(request, rsp, ctx);
                    Ok(())
                } else {
                    //协议要求服务端错误断连
                    w.write(b"SERVER_ERROR mcq not available\r\n")?;
                    Err(Error::Quit)
                }
            }
        }
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

impl MsgQue {
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
    #[inline]
    fn val_len(data: ds::RingSlice, start: usize, end: usize) -> Result<usize> {
        let Some(space) = data.rfind_r(start..end, b' ') else {
            return Err(Error::ProtocolNotSupported);
        };
        let Some(val_len) = data.try_str_num(space + 1..end) else {
            return Err(Error::ProtocolNotSupported);
        };
        Ok(val_len)
    }
}
