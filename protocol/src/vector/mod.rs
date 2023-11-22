mod command;
pub mod flager;
mod query_result;
mod reqpacket;
mod rsppacket;

use crate::{
    Command, Commander, Error, HashedCommand, Metric, MetricItem, Protocol, RequestProcessor,
    Result, Stream, Writer,
};
use ds::RingSlice;
use sharding::hash::Hash;

use self::query_result::{QueryResult, Text};
use self::reqpacket::RequestPacket;
use self::rsppacket::ResponsePacket;

use crate::kv::client::Client;
use crate::kv::common::query_result::Or;
use crate::kv::error;
use crate::kv::HandShakeStatus;
use crate::HandShake;

#[derive(Clone, Default)]
pub struct Vector;

impl Protocol for Vector {
    fn parse_request<S: Stream, H: Hash, P: RequestProcessor>(
        &self,
        stream: &mut S,
        alg: &H,
        process: &mut P,
    ) -> Result<()> {
        // 解析redis格式的指令，构建结构化的redis vector 内部cmd，要求：方便转换成sql
        let mut packet = RequestPacket::new(stream);
        match self.parse_request_inner(&mut packet, alg, process) {
            Ok(_) => Ok(()),
            Err(Error::ProtocolIncomplete) => {
                // 如果解析数据不够，提前reserve stream的空间
                packet.reserve_stream_buff();
                Ok(())
            }
            e => {
                log::warn!("redis parsed err: {:?}, req: {:?} ", e, packet.inner_data());
                e
            }
        }
    }

    fn config(&self) -> crate::Config {
        crate::Config {
            need_auth: true,
            ..Default::default()
        }
    }

    fn handshake(
        &self,
        stream: &mut impl Stream,
        option: &mut crate::ResOption,
    ) -> crate::Result<HandShake> {
        log::debug!("+++ vector handshake");
        match self.handshake_inner(stream, option) {
            Ok(h) => Ok(h),
            Err(crate::Error::ProtocolIncomplete) => Ok(HandShake::Continue),
            Err(e) => {
                log::warn!("+++ found err when shake hand:{:?}", e);
                Err(e)
            }
        }
    }

    fn parse_response<S: Stream>(&self, data: &mut S) -> Result<Option<Command>> {
        log::debug!("+++ vector recv mysql response:{:?}", data.slice());
        let mut rsp_packet = ResponsePacket::new(data, None);

        // 解析完毕rsp后，除了数据未读完的场景，其他不管是否遇到err，都要进行take
        match self.parse_response_inner2(&mut rsp_packet) {
            Ok(cmd) => Ok(Some(cmd)),
            Err(crate::Error::ProtocolIncomplete) => Ok(None),
            Err(e) => {
                // 非MysqlError需要日志并外层断连处理
                log::error!("+++ err when parse mysql response: {:?}", e);
                Err(e.into())
            }
        }
    }

    fn write_response<C, W, M, I>(
        &self,
        ctx: &mut C,
        response: Option<&mut Command>,
        w: &mut W,
    ) -> crate::Result<()>
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

        log::debug!(
            "+++ send to client rsp, req:{:?}, resp:{:?}",
            ctx.request(),
            response
        );
        // 非sentonly返回空，报错，外部断连接
        if response.is_none() {
            log::warn!("+++ vector response is none req:{:?}", ctx.request());
            return Err(crate::Error::OpCodeNotSupported(255)); // TODO
        }
        if let Some(response) = response {
            if !response.ok() {
                log::debug!(
                    "+++ vector resp error req: req:{:?}, resp:{:?}",
                    ctx.request(),
                    response
                );

                // 对于非ok的响应，需要构建rsp，发给client
                w.write("-ERR ".as_bytes())?;
                w.write_slice(response, 0)?; // mysql返回的错误信息
                w.write("\r\n".as_bytes())?;
                return Ok(());
            } else {
                // response已封装为redis协议。正常响应有三种：
                // 1. 只返回影响的行数
                // 2. 一行或多行数据
                // 3. 结果为空
                w.write_slice(response, 0)?; // value
            }
        }

        Ok(())
    }
}

impl Vector {
    #[inline]
    fn parse_request_inner<S: Stream, H: Hash, P: RequestProcessor>(
        &self,
        packet: &mut RequestPacket<S>,
        _alg: &H,
        process: &mut P,
    ) -> Result<()> {
        log::debug!("+++ rec kvector req:{:?}", packet.inner_data());
        while packet.available() {
            packet.parse_bulk_num()?;
            let flag = packet.parse_cmd()?;
            // 构建cmd，准备后续处理
            let cmd = packet.take();
            let hash = 0;
            let cmd = HashedCommand::new(cmd, hash, flag);
            process.process(cmd, true);
        }

        Ok(())
    }

    fn handshake_inner<S: Stream>(
        &self,
        stream: &mut S,
        option: &mut crate::ResOption,
    ) -> crate::Result<HandShake> {
        log::debug!("+++ recv vector handshake packet:{:?}", stream.slice());
        let client = Client::from_user_pwd(option.username.clone(), option.token.clone());
        let mut packet = ResponsePacket::new(stream, Some(client));

        match packet.ctx().status {
            HandShakeStatus::Init => {
                packet.proc_handshake()?;
                packet.ctx().status = HandShakeStatus::InitialhHandshakeResponse;
                Ok(HandShake::Continue)
            }
            HandShakeStatus::InitialhHandshakeResponse => {
                packet.proc_auth()?;
                packet.ctx().status = HandShakeStatus::AuthSucceed;
                log::debug!("+++ proc auth succeed!");
                Ok(HandShake::Success)
            }

            HandShakeStatus::AuthSucceed => Ok(HandShake::Success),
        }
    }

    /// 解析mysql响应。mysql协议比较复杂，stream不能随意take，得等到最终解析完毕后，才能统一take走；
    /// 本方法内，不管什么类型的包，只要不是Err，在返回响应前都得take
    fn parse_response_inner<'a, S: crate::Stream>(
        &self,
        rsp_packet: &'a mut ResponsePacket<'a, S>,
    ) -> crate::Result<Command> {
        // 首先parse meta，对于UnhandleResponseError异常，需要构建成响应返回
        let meta = match rsp_packet.parse_result_set_meta() {
            Ok(meta) => meta,
            Err(error::Error::UnhandleResponseError(emsg)) => {
                // 对于UnhandleResponseError，需要构建rsp，发给client
                let cmd = rsp_packet.build_final_rsp_cmd(false, emsg);
                return Ok(cmd);
            }
            Err(e) => return Err(e.into()),
        };

        // 如果是只有meta的ok packet，直接返回影响的列数，如insert/delete/update
        if let Or::B(ok) = meta {
            let n = ok.affected_rows();
            let cmd = rsp_packet.build_final_affected_rows_rsp_cmd(n);
            return Ok(cmd);
        }

        // 解析meta后面的rows，返回列记录，如select
        let mut query_result: QueryResult<Text, S> = QueryResult::new(rsp_packet, meta);
        match query_result.parse_rows() {
            Ok(cmd) => Ok(cmd),
            Err(error::Error::UnhandleResponseError(emsg)) => {
                // 对于UnhandleResponseError，需要构建rsp，发给client
                let cmd = query_result.build_final_rsp_cmd(false, emsg);
                Ok(cmd)
            }
            Err(e) => Err(e.into()),
        }
    }

    fn parse_response_inner2<'a, S: crate::Stream>(
        &self,
        rsp_packet: &'a mut ResponsePacket<'a, S>,
    ) -> crate::Result<Command> {
        // 首先parse meta，对于UnhandleResponseError异常，需要构建成响应返回
        let meta = match rsp_packet.parse_result_set_meta() {
            Ok(meta) => meta,
            Err(error::Error::UnhandleResponseError(emsg)) => {
                // 对于UnhandleResponseError，需要构建rsp，发给client
                let cmd = rsp_packet.build_final_rsp_cmd(false, emsg);
                return Ok(cmd);
            }
            Err(e) => return Err(e.into()),
        };

        log::debug!("+++ parsed meta succeed!");

        // 如果是只有meta的ok packet，直接返回影响的列数，如insert/delete/update
        if let Or::B(ok) = meta {
            let n = ok.affected_rows();
            let cmd = rsp_packet.build_final_affected_rows_rsp_cmd(n);
            return Ok(cmd);
        }

        // 解析meta后面的rows，返回列记录，如select
        // 有可能多行数据，直接build成
        let mut query_result: QueryResult<Text, S> = QueryResult::new(rsp_packet, meta);
        match query_result.parse_rows2() {
            Ok(cmd) => Ok(cmd),
            Err(error::Error::UnhandleResponseError(emsg)) => {
                // 对于UnhandleResponseError，需要构建rsp，发给client
                let cmd = query_result.build_final_rsp_cmd(false, emsg);
                Ok(cmd)
            }
            Err(e) => Err(e.into()),
        }
    }
}

// pub struct RingSliceIter {}
// impl Iterator for RingSliceIter {
//     type Item = RingSlice;
//     fn next(&mut self) -> Option<Self::Item> {
//         todo!()
//     }
// }
// pub struct ConditionIter {}
// impl Iterator for ConditionIter {
//     type Item = Condition;
//     fn next(&mut self) -> Option<Self::Item> {
//         todo!()
//     }
// }

struct LikeByMe {}

pub enum Opcode {}

pub(crate) const COND_ORDER: &str = "ORDER";
pub(crate) const COND_LIMIT: &str = "LIMIT";

#[derive(Debug, Clone, Default)]
pub struct Condition {
    pub field: RingSlice,
    pub op: RingSlice,
    pub value: RingSlice,
}

impl Condition {
    /// 构建一个condition
    #[inline]
    pub(crate) fn new(field: RingSlice, op: RingSlice, value: RingSlice) -> Self {
        Self { field, op, value }
    }
}

// pub enum Order {
//     ASC,
//     DESC,
// }
#[derive(Debug, Clone, Default)]
pub struct Order {
    pub field: RingSlice,
    pub order: RingSlice,
}
// pub struct Orders {
//     pub field: RingSliceIter,
//     pub order: Order,
// }

#[derive(Debug, Clone, Default)]
pub struct Limit {
    //无需转成int，可直接拼接
    pub offset: RingSlice,
    pub limit: RingSlice,
}

pub const OP_VRANGE: u16 = 0;
//非迭代版本，代价是内存申请。如果采取迭代版本，需要重复解析一遍，重复解析可以由parser实现，topo调用
#[derive(Debug, Clone, Default)]
pub struct VectorCmd {
    pub keys: Vec<RingSlice>,
    pub fields: Vec<(RingSlice, RingSlice)>,
    pub wheres: Vec<Condition>,
    pub order: Order,
    pub limit: Limit,
}
