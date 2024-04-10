use crate::{
    Command, Commander, Error, Flag, HashedCommand, Metric, MetricItem, OpCode, Operation,
    Protocol, RequestProcessor, Result, Stream, Writer,
};
use sharding::hash::{Bkdr, Hash, UppercaseHashKey};

#[derive(Debug)]
pub enum McqError {
    ReqInvalid,
    RspInvalid,
}

impl Into<Error> for McqError {
    #[inline]
    fn into(self) -> Error {
        Error::Mcq(self)
    }
}

const OP_GET: u16 = 0;
const OP_SET: u16 = 1;
const OP_STATS: u16 = 2;
const OP_VERSION: u16 = 3;
const OP_QUIT: u16 = 4;
const OP_UNKNOWN: u16 = 5;

#[derive(Clone, Copy, Debug, Default)]
struct CommandProperties {
    op_code: OpCode,
    op: Operation,
    padding_rsp: &'static [u8],
    noforward: bool,
}

#[inline]
fn get_cfg_by_name<'a>(cmd: &ds::RingSlice) -> crate::Result<&'a CommandProperties> {
    todo!("");
}
#[inline]
fn get_cfg<'a>(op_code: u16) -> crate::Result<&'a CommandProperties> {
    SUPPORTED
        .get(op_code as usize)
        .ok_or(crate::Error::ProtocolNotSupported)
}

const PADDING_RSP_TABLE: [&[u8]; 4] = [
    b"",
    b"SERVER_ERROR mcq not available\r\n",
    b"VERSION 0.0.1\r\n",
    b"STAT supported later\r\nEND\r\n",
];

// for (name, req_type, op, padding_rsp, noforward) in vec![
//     ("get", RequestType::Get, Get, 1, false),
//     ("set", RequestType::Set, Store, 1, false),
//     ("stats", RequestType::Stats, Meta, 3, true),
//     ("version", RequestType::Version, Meta, 2, true),
//     ("quit", RequestType::Quit, Meta, 0, true),
// ]
use Operation::*;
static SUPPORTED: [CommandProperties; 5] = [
    CommandProperties {
        op_code: OP_GET,
        op: Get,
        padding_rsp: PADDING_RSP_TABLE[1],
        noforward: false,
    },
    CommandProperties {
        op_code: OP_SET,
        op: Store,
        padding_rsp: PADDING_RSP_TABLE[1],
        noforward: false,
    },
    CommandProperties {
        op_code: OP_STATS,
        op: Meta,
        padding_rsp: PADDING_RSP_TABLE[3],
        noforward: true,
    },
    CommandProperties {
        op_code: OP_VERSION,
        op: Meta,
        padding_rsp: PADDING_RSP_TABLE[2],
        noforward: true,
    },
    CommandProperties {
        op_code: OP_QUIT,
        op: Meta,
        padding_rsp: PADDING_RSP_TABLE[0],
        noforward: true,
    },
];

pub type MsgQue = McqText;

#[derive(Clone, Default)]
pub struct McqText;

impl Protocol for McqText {
    #[inline]
    fn parse_request<S: Stream, H: Hash, P: RequestProcessor>(
        &self,
        stream: &mut S,
        alg: &H,
        process: &mut P,
    ) -> Result<()> {
        let data = stream.slice();
        let mut oft = 0;
        while let Some(lfcr) = data.find_lf_cr(oft) {
            let op_code = if data.start_with(oft, b"get") {
                OP_GET
            // <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
            // 命令之后第四个空格是数据长度
            } else if data.start_with(oft, b"set") {
                let Some(space) = data.skip_space(oft + 3, 4) else {
                    return Err(Error::ProtocolNotSupported);
                };
                let Some(data_len) = data.try_str_num(space..lfcr + 2) else {
                    return Err(Error::ProtocolNotSupported);
                };
                OP_SET
            } else if data.start_with(oft, b"stats") {
                OP_STATS
            } else if data.start_with(oft, b"version") {
                OP_VERSION
            } else if data.start_with(oft, b"quit") {
                OP_QUIT
            } else {
                OP_UNKNOWN
            };
        }

        Ok(())
    }

    #[inline]
    fn parse_response<S: Stream>(&self, data: &mut S) -> Result<Option<Command>> {
        todo!("");
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
        let cfg = get_cfg(request.op_code())?;

        // 对于quit指令，直接返回Err断连
        // if cfg.quit {
        //     // mc协议的quit，直接断连接
        //     return Err(crate::Error::Quit);
        // }

        if let Some(rsp) = response {
            // 不再创建local rsp，所有server响应的rsp data长度应该大于0
            debug_assert!(rsp.len() > 0, "req:{:?}, rsp:{:?}", request, rsp);
            log::debug!("+++ write mq:{}", rsp.as_string_lossy());
            w.write_slice(rsp, 0)?;
        } else {
            let padding = cfg.padding_rsp;
            log::debug!("+++ write mq padding:{:?}", padding);
            w.write(padding)?;
            // TODO 写失败尚没有统计（还没merge进来？），暂时先和dev保持一致 fishermen
        }
        Ok(())
    }
}
