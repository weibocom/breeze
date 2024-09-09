pub mod attachment;
mod command;
pub(crate) mod error;
pub mod flager;
pub mod mysql;
mod packet;
mod query_result;
pub mod redis;
mod reqpacket;
mod rsppacket;

use std::fmt::Write;

use crate::{
    Attachment, Command, Commander, Error, HashedCommand, Metric, MetricItem, Protocol,
    RequestProcessor, Result, Stream, Writer,
};
use attachment::{AttachType, Route};
use chrono::NaiveDate;
use ds::RingSlice;
use sharding::hash::Hash;

use self::attachment::VectorAttach;
use self::packet::RedisPack;
use self::reqpacket::RequestPacket;
use self::rsppacket::ResponsePacket;
use crate::kv::client::Client;
use crate::kv::{ContextStatus, HandShakeStatus};
use crate::HandShake;
pub use command::CommandType;

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
            Err(Error::ProtocolIncomplete(0)) => {
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
            Err(crate::Error::ProtocolIncomplete(0)) => Ok(HandShake::Continue),
            Err(e) => {
                log::warn!("+++ found err when shake hand:{:?}", e);
                Err(e)
            }
        }
    }

    fn parse_response<S: Stream>(&self, data: &mut S) -> Result<Option<Command>> {
        log::debug!("+++ vector recv mysql response:{:?}", data.slice());
        let mut rsp_packet = ResponsePacket::new(data);

        // 解析完毕rsp后，除了数据未读完的场景，其他不管是否遇到err，都要进行take
        match self.parse_response_inner(&mut rsp_packet) {
            Ok(cmd) => Ok(Some(cmd)),
            Err(crate::Error::ProtocolIncomplete(0)) => {
                rsp_packet.reserve();
                Ok(None)
            }
            Err(e) => {
                log::error!("+++ err when parse mysql response: {:?}", e);
                Err(e)
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

        if let Some(response) = response {
            log::debug!("+++ send to client {:?} => {:?}", ctx.request(), response);
            if !response.ok() {
                // 对于非ok的响应，需要构建rsp，发给client
                w.write("-ERR ".as_bytes())?;
                w.write_slice(response, 0)?; // mysql返回的错误信息
                w.write("\r\n".as_bytes())?;
            } else {
                if ctx.attachment().is_some() {
                    let vec_attach =
                        VectorAttach::attach(ctx.attachment().expect("vector attache"));
                    match vec_attach.attch_type() {
                        AttachType::Retrieve => {
                            // 有attachment: 组装rsp: header(vec[0]) + *body_tokens + vec[1..]
                            let attach = vec_attach.retrieve_attach();
                            if attach.body_token_count() > 0 {
                                w.write(attach.header())?;
                                w.write(format!("*{}\r\n", attach.body_token_count()).as_bytes())?;
                                for b in attach.body() {
                                    w.write(b.as_slice())?;
                                }
                            } else {
                                // 返回空
                                w.write("$-1\r\n".as_bytes())?;
                            }
                        }
                        AttachType::Store => {
                            let attach = vec_attach.store_attach();
                            w.write(format!(":{}\r\n", attach.affected_rows).as_bytes())?;
                        }
                        _ => {
                            panic!("bad attach type");
                        }
                    }
                } else {
                    // 无attachment: response已封装为redis协议。正常响应有三种：
                    // 1. 只返回影响的行数
                    // 2. 一行或多行数据
                    // 3. 结果为空
                    if let Some(ref header) = response.header {
                        if header.rows > 0 {
                            w.write(header.header.as_ref())?;
                            w.write(format!("*{}\r\n", header.rows * header.columns).as_bytes())?;
                        }
                    }
                    w.write_slice(response, 0)?; // value
                }
            }
            return Ok(());
        }

        // 没有响应，考虑quit或返回占位rsp
        let op_code = ctx.request().op_code();
        let cfg = command::get_cfg(op_code).expect("kv cfg");

        // quit 直接返回Err，外层断连接
        if cfg.quit {
            return Err(crate::Error::Quit); // TODO
        }

        use crate::kv::KVCtx;
        let response = match ctx.ctx().ctx().error {
            ContextStatus::TopInvalid => b"-ERR invalid request: year out of index\r\n".as_slice(),
            ContextStatus::ReqInvalid => b"-ERR invalid request\r\n".as_slice(),
            ContextStatus::Ok => cfg.padding_rsp.as_bytes(),
        };

        // 其他场景返回padding rsp
        w.write(response)?;
        log::debug!("+++ send to client padding {:?}", ctx.request());
        Ok(())
    }

    // 将中间响应放到attachment中，方便后续继续查询
    // 先收集si信息，再收集body
    // 返回值：是否继续下一轮
    #[inline]
    fn update_attachment(&self, attachment: &mut Attachment, response: &mut Command) -> bool {
        assert!(response.ok());
        let vec_attach = VectorAttach::attach_mut(attachment);
        //收到响应就算ok，响应有问题也不会发送到topo了
        vec_attach.rsp_ok = true;

        match vec_attach.attch_type() {
            AttachType::Retrieve => {
                assert!(response.header.is_some(), "rsp:{}", response);
                let attach = vec_attach.retrieve_attach_mut();
                let body_data = response.data().0.to_vec();
                let header = response.header.as_mut().expect("rsp header");

                if attach.is_empty() {
                    // TODO 简化swap操作，注意功能验证 fishermen
                    // let mut header_data = Vec::new();
                    // let header = &mut response.header;
                    // mem::swap(&mut header_data, &mut header.header);
                    attach.swap_header_data(&mut header.header);
                }

                // TODO 先打通，此处的内存操作需要考虑优化 fishermen
                match attach.has_si() {
                    true => {
                        if header.rows > 0 {
                            // let header = &response.header;
                            attach.attach_body(body_data, header.rows, header.columns);
                        }
                        attach.left_count != 0
                    }
                    // 按si解析响应: 未成功获取有效si信息或者解析si失败，并终止后续请求
                    false => response.count() != 0 && attach.attach_si(response),
                }
            }
            AttachType::Store => {
                let store_attach = vec_attach.store_attach_mut();
                store_attach.incr_affected_rows(response.count() as u16);
                true
            }
            _ => {
                panic!("malformed attach");
            }
        }
    }
    #[inline]
    fn drop_attach(&self, att: Attachment) {
        let _ = VectorAttach::from(att);
    }
}

impl Vector {
    #[inline]
    fn parse_request_inner<S: Stream, H: Hash, P: RequestProcessor>(
        &self,
        packet: &mut RequestPacket<S>,
        alg: &H,
        process: &mut P,
    ) -> Result<()> {
        log::debug!("+++ rec kvector req:{:?}", packet.inner_data());
        while packet.available() {
            packet.parse_bulk_num()?;
            let cfg = packet.parse_cmd_properties()?;
            let mut flag = cfg.flag();
            let key = packet.parse_cmd(cfg, &mut flag)?;

            // 构建cmd，准备后续处理
            let cmd = packet.take();
            let hash = packet.hash(key, alg);
            log::debug!("+++ kvector/{} key:{:?}/{}", cfg.name, key, hash);
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
        let mut packet = ResponsePacket::new(stream);
        match packet.ctx().status {
            HandShakeStatus::Init => {
                let mut client =
                    Client::from_user_pwd(option.username.clone(), option.token.clone());
                packet.proc_handshake(&mut client)?;
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

    /// 消除了rsppacket 和 query_result的循环依赖，代价就是不支持存储过程 fishermen
    fn parse_response_inner<'a, S: crate::Stream>(
        &self,
        rsp_packet: &mut ResponsePacket<S>,
    ) -> crate::Result<Command> {
        // parse result set，对于UnhandleResponseError异常，需要构建成响应返回
        match rsp_packet.parse_result_set() {
            Ok(cmd) => Ok(cmd),
            Err(crate::kv::error::Error::UnhandleResponseError(emsg)) => {
                // 对于UnhandleResponseError，需要构建rsp，发给client
                let cmd = rsp_packet.build_final_rsp_cmd(false, RedisPack::with_simple(emsg));
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

pub enum Opcode {}

pub(crate) const COND_ORDER: &[u8] = b"ORDER";
pub(crate) const COND_LIMIT: &[u8] = b"LIMIT";
pub(crate) const COND_GROUP: &[u8] = b"GROUP";

const DEFAULT_LIMIT: usize = 15;

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
#[derive(Debug, Clone, Default)]
pub struct GroupBy {
    pub fields: RingSlice,
}

pub type Field = (RingSlice, RingSlice);

//非迭代版本，代价是内存申请。如果采取迭代版本，需要重复解析一遍，重复解析可以由parser实现，topo调用
#[derive(Debug, Clone, Default)]
pub struct VectorCmd {
    pub route: Route,
    pub cmd: CommandType,
    pub keys: Vec<RingSlice>,
    pub fields: Vec<Field>,
    pub wheres: Vec<Condition>,
    pub order: Order,
    pub limit: Limit,
    pub group_by: GroupBy,
}

impl VectorCmd {
    #[inline(always)]
    pub fn limit(&self) -> usize {
        match self.limit.limit.try_str_num(..) {
            Some(limit) => limit,
            None => DEFAULT_LIMIT,
        }
    }
}

/// field 字段的值，对于‘field’关键字，值是｜分隔的field names，否则就是二进制value
#[derive(Debug, Clone)]
pub enum FieldVal {
    Names(Vec<RingSlice>),
    Val(RingSlice),
}

impl FieldVal {
    // TODO 需要在parse时，增加协议判断 fishermen
    #[inline(always)]
    pub fn names(&self) -> &Vec<RingSlice> {
        match self {
            Self::Names(names) => names,
            _ => panic!("mustn't has field bin val"),
        }
    }

    // TODO 需要在parse时，增加协议判断 fishermen
    #[inline(always)]
    pub fn val(&self) -> &RingSlice {
        match self {
            Self::Val(val) => val,
            _ => panic!("mustn't has field names"),
        }
    }

    /// check names，如果names不存在，则返回异常
    #[inline(always)]
    pub fn has_names(&self) -> bool {
        match self {
            Self::Names(_) => true,
            _ => false,
        }
    }

    /// check val，如果val不存在，则返回异常
    pub fn has_val(&self) -> bool {
        match self {
            Self::Val(_) => true,
            _ => false,
        }
    }
}

pub const DATE_YYMM: &str = "yymm";
pub const DATE_YYMMDD: &str = "yymmdd";

#[derive(Debug, Clone)]
pub enum Postfix {
    YYMM,
    YYMMDD,
}

impl TryInto<Postfix> for &str {
    type Error = Error;
    fn try_into(self) -> std::result::Result<Postfix, Self::Error> {
        match self.to_lowercase().as_str() {
            DATE_YYMM => Ok(Postfix::YYMM),
            DATE_YYMMDD => Ok(Postfix::YYMMDD),
            _ => Err(Error::RequestProtocolInvalid),
        }
    }
}

pub enum KeysType<'a> {
    Keys(&'a String),
    Time,
}

pub trait Strategy {
    fn keys(&self) -> &[String];
    //todo 通过代理类型实现
    fn keys_with_type(&self) -> Box<dyn Iterator<Item = KeysType> + '_>;
    fn write_database_table(&self, buf: &mut impl Write, date: &NaiveDate, hash: i64);
    fn write_si_database_table(&self, buf: &mut impl Write, hash: i64);
    fn batch(&self, limit: u64, vcmd: &VectorCmd) -> u64;
    fn si_cols(&self) -> &[String];
}
