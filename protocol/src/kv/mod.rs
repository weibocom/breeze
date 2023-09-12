mod client;
pub mod common;
mod mcpacket;

mod error;
mod packet;
mod reqpacket;
mod rsppacket;

mod mc2mysql;
pub use mc2mysql::{MysqlBuilder, Strategy};

use self::common::proto::Text;
use self::common::query_result::{Or, QueryResult};
use self::common::row::convert::from_row;

use self::error::Result;
use bytes::BufMut;
pub use common::proto::codec::PacketCodec;

use self::mcpacket::PacketPos;
use self::mcpacket::RespStatus;
pub use self::mcpacket::*;
use self::rsppacket::ResponsePacket;

use super::Flag;
use super::Protocol;
use crate::kv::client::Client;
use crate::kv::error::Error;
use crate::Command;
use crate::HandShake;
use crate::HashedCommand;
use crate::RequestProcessor;
use crate::Stream;
use ds::{MemGuard, RingSlice};

use sharding::hash::Hash;

pub mod prelude {

    #[doc(inline)]
    pub use crate::kv::common::row::convert::FromRow;
    #[doc(inline)]
    pub use crate::kv::common::row::ColumnIndex;
    #[doc(inline)]
    pub use crate::kv::common::value::convert::{ConvIr, FromValue, ToValue};

    /// Trait for protocol markers [`crate::Binary`] and [`crate::Text`].
    pub(crate) trait Protocol: crate::kv::common::query_result::Protocol {}

    impl Protocol for crate::kv::common::proto::Binary {}
    impl Protocol for crate::kv::common::proto::Text {}
}

#[derive(Clone, Default)]
pub struct Kv {}

#[derive(Debug, Clone, Copy)]
pub(self) enum HandShakeStatus {
    #[allow(dead_code)]
    Init,
    InitialhHandshakeResponse,
    AuthSucceed,
}

impl Protocol for Kv {
    fn handshake(
        &self,
        stream: &mut impl Stream,
        option: &mut crate::ResOption,
    ) -> crate::Result<HandShake> {
        match self.handshake_inner(stream, option) {
            Ok(h) => Ok(h),
            Err(crate::Error::ProtocolIncomplete) => Ok(HandShake::Continue),
            Err(e) => {
                log::warn!("+++ found err when shake hand:{:?}", e);
                Err(e)
            }
        }
    }

    fn config(&self) -> crate::Config {
        crate::Config {
            need_auth: true,
            ..Default::default()
        }
    }
    // 解析mc binary协议，在发送端进行协议转换
    fn parse_request<S: Stream, H: Hash, P: RequestProcessor>(
        &self,
        stream: &mut S,
        alg: &H,
        process: &mut P,
    ) -> crate::Result<()> {
        assert!(stream.len() > 0, "mc req: {:?}", stream.slice());
        log::debug!("+++ recv mysql-mc req:{:?}", stream.slice());

        // 直接解析mc协议，待有额外逻辑，再考虑封装解析过程
        while stream.len() >= mcpacket::HEADER_LEN {
            let mut req = stream.slice();
            // 如果magic都不对，先判定为攻击，直接断连接，不返回任何响应
            req.check_request()?;
            let packet_len = req.packet_len();
            if req.len() < packet_len {
                stream.reserve(packet_len - req.len());
                break;
            }

            if req.key_len() > 0 {
                self.validate_request(&req)?;
            }

            let last = !req.quiet_get(); // 须在map_op之前获取
            let cmd = req.operation();
            let op_code = req.map_op(); // 把quite get请求，转换成单个的get请求s
            let mut flag = Flag::from_op(op_code as u16, cmd);
            // flag.set_try_next_type(req.try_next_type());
            flag.set_sentonly(req.sentonly());
            flag.set_noforward(req.noforward());

            let guard = stream.take(packet_len);
            let hash = req.hash(alg);
            let cmd = HashedCommand::new(guard, hash, flag);
            assert!(!cmd.quiet_get());
            process.process(cmd, last);
        }

        Ok(())
    }

    //fn build_request(&self, req: &mut HashedCommand, new_req: MemGuard) {
    //    req.reshape(new_req);
    //}

    // 解析mysql response；在write response的时候，再进行协议格式转换
    fn parse_response<S: crate::Stream>(&self, data: &mut S) -> crate::Result<Option<Command>> {
        log::debug!("+++ recv mysql response:{:?}", data.slice());
        let mut rsp_packet = ResponsePacket::new(data, None);

        // 解析完毕rsp后，除了数据未读完的场景，其他不管是否遇到err，都要进行take
        match self.parse_response_inner(&mut rsp_packet) {
            Ok(cmd) => Ok(cmd),

            Err(Error::ProtocolIncomplete) => Ok(None),
            Err(Error::UnhandleResponseError(s)) => {
                // 异常响应通过标准处理流程，所以此处不需要进行协议包转换
                let mem = ds::MemGuard::from_vec(s);
                let cmd = Command::from(false, mem);
                log::debug!("+++ parse mysql response err to cmd:{:?}", cmd);
                Ok(Some(cmd))
            }
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
        response: Option<&mut crate::Command>,
        w: &mut W,
    ) -> crate::Result<()>
    where
        W: crate::Writer,
        C: crate::Commander<M, I>,
        M: crate::Metric<I>,
        I: crate::MetricItem,
    {
        // sendonly 直接返回
        if ctx.request().sentonly() {
            assert!(response.is_none(), "req:{:?}", ctx.request());
            return Ok(());
        }

        let old_op_code = ctx.request().op_code() as u8;

        // 如果原始请求是quite_get请求，并且not found，则不回写。
        // if let Some(rsp) = response {
        //     log::debug!(
        //         "+++ sent to client for req:{:?}, rsp:{:?}",
        //         ctx.request(),
        //         rsp
        //     );
        //     // mysql 请求到正确的数据，才会转换并write
        //     self.write_mc_packet(ctx.request(), Some(rsp), w)?;
        //     return Ok(());
        // }

        // 先进行metrics统计
        //self.metrics(ctx.request(), None, ctx);
        log::debug!("+++ send to client rsp, req:{:?}", ctx.request(),);
        match old_op_code {
            // noop: 第一个字节变更为Response，其他的与Request保持一致
            OP_NOOP => {
                w.write_u8(RESPONSE_MAGIC)?;
                w.write_slice(ctx.request(), 1)?;
            }

            //version: 返回固定rsp
            OP_VERSION => w.write(&VERSION_RESPONSE)?,

            // stat：返回固定rsp
            OP_STAT => w.write(&STAT_RESPONSE)?,

            // quit/quitq 无需返回rsp
            OP_QUIT | OP_QUITQ => return Err(crate::Error::Quit),

            // quite get 请求，无需返回任何rsp，但没实际发送，rsp_ok设为false
            // OP_GETQ | OP_GETKQ => return Ok(()),
            // 0x09 | 0x0d => return Ok(()),

            // set: mc status设为 Item Not Stored,status设为false
            OP_SET | OP_ADD | OP_GET | OP_GETK | OP_DEL | OP_GETQ | OP_GETKQ => {
                log::debug!(
                    "+++ sent to client for req:{:?}, rsp:{:?}",
                    ctx.request(),
                    response
                );
                self.write_mc_response(ctx.request(), response.map(|r| &*r), w)?
            }
            // self.build_empty_response(RespStatus::NotStored, req)

            // get/gets，返回key not found 对应的0x1
            // OP_GET => w.write(&self.build_empty_response(RespStatus::NotFound, ctx.request()))?,
            // OP_DEL => w.write(&self.build_empty_response(RespStatus::NotFound, ctx.request()))?,

            // self.build_empty_response(RespStatus::NotFound, req)

            // 未知异常，外层直接断连接
            _ => {
                log::warn!(
                    "+++ mysql NoResponseFound req: {}/{:?}",
                    old_op_code,
                    ctx.request()
                );
                return Err(crate::Error::OpCodeNotSupported(old_op_code as u16));
            }
        }
        Ok(())
    }

    fn build_writeback_request<C, M, I>(
        &self,
        _ctx: &mut C,
        _response: &crate::Command,
        _: u32,
    ) -> Option<crate::HashedCommand>
    where
        C: crate::Commander<M, I>,
        M: crate::Metric<I>,
        I: crate::MetricItem,
    {
        None
    }
}

impl Kv {
    fn handshake_inner<S: Stream>(
        &self,
        stream: &mut S,
        option: &mut crate::ResOption,
    ) -> crate::Result<HandShake> {
        log::debug!("+++ recv mysql handshake packet:{:?}", stream.slice());
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

    // 根据req构建response，status为mc协议status，共11种
    #[inline]
    #[allow(dead_code)]
    fn build_empty_response(&self, status: RespStatus, req: &HashedCommand) -> [u8; HEADER_LEN] {
        let mut response = [0; HEADER_LEN];
        response[PacketPos::Magic as usize] = RESPONSE_MAGIC;
        response[PacketPos::Opcode as usize] = req.op();
        response[PacketPos::Status as usize + 1] = status as u8;
        //复制 Opaque
        for i in PacketPos::Opaque as usize..PacketPos::Opaque as usize + 4 {
            response[i] = req.at(i);
        }
        response
    }

    /// 对request进行校验
    #[inline(always)]
    fn validate_request(&self, request: &RingSlice) -> crate::Result<()> {
        // 当前只检查key，后续需要增加逻辑在此处加
        let key = request.key();
        for i in 0..key.len() {
            if !key.at(i).is_ascii_digit() {
                log::warn!("+++ found malformed mysql-mc packet:{:?}", request);
                let err_packet = self.build_error_rsp(request, error::REQ_INVALID_KEY);
                return Err(Error::RequestInvalidKey(err_packet).into());
            }
        }
        Ok(())
    }

    /// 解析mysql响应。mysql协议比较复杂，stream不能随意take，得等到最终解析完毕后，才能统一take走；
    /// 本方法内，不管什么类型的包，返回之前，都得take
    fn parse_response_inner<'a, S: crate::Stream>(
        &self,
        rsp_packet: &'a mut ResponsePacket<'a, S>,
    ) -> Result<Option<Command>> {
        // let mut result_set = self.parse_result_set(rsp_packet)?;
        let meta = rsp_packet.parse_result_set_meta()?;

        // 返回影响的列数，如insert/delete/update
        if let Or::B(ok) = meta {
            let n = ok.affected_rows();
            let mem = MemGuard::from_vec(n.to_be_bytes().to_vec());
            let cmd = Command::from_ok(mem);
            rsp_packet.take();
            log::debug!("+++ parse_response_inner okpacket respons cmd:{:?}", cmd);
            return Ok(Some(cmd));
        }

        // 返回值有列数据的操作，如select
        let mut query_result: QueryResult<Text, S> = QueryResult::new(rsp_packet, meta);
        let collector = |mut acc: Vec<Vec<u8>>, row| {
            acc.push(from_row(row));
            acc
        };
        let mut result_set = query_result.scan_rows(Vec::with_capacity(4), collector)?;
        let status = result_set.len() > 0;
        let row: Vec<u8> = if status {
            //现在只支持单key，remove也不影响
            result_set.remove(0)
        } else {
            b"not found".to_vec()
        };

        let mem = MemGuard::from_vec(row);
        let cmd = Command::from(status, mem);
        log::debug!("++++ recv kv resp:{:?}", cmd);
        Ok(Some(cmd))
    }

    // #[allow(dead_code)]
    // fn parse_result_set<'a, S>(
    //     &self,
    //     rsp_packet: &'a mut ResponsePacket<'a, S>,
    // ) -> Result<Vec<Vec<u8>>>
    // where
    //     S: crate::Stream,
    // {
    //     let meta = rsp_packet.parse_result_set_meta()?;
    //     let mut query_result: QueryResult<Text, S> = QueryResult::new(rsp_packet, meta);
    //     let collector = |mut acc: Vec<Vec<u8>>, row| {
    //         acc.push(from_row(row));
    //         acc
    //     };
    //     let result_set = query_result.scan_rows(Vec::with_capacity(4), collector)?;
    //     Ok(result_set)
    // }

    #[inline]
    fn write_mc_packet<W>(
        &self,
        opcode: u8,
        status: RespStatus,
        key: Option<RingSlice>,
        extra: Option<u32>,
        response: Option<&crate::Command>,
        w: &mut W,
    ) -> crate::Result<()>
    where
        W: crate::Writer,
    {
        w.write_u8(mcpacket::Magic::Response as u8)?; //magic 1byte
        w.write_u8(opcode)?; // opcode 1 byte

        let key_len = key.as_ref().map_or(0, |r| r.len());
        w.write_u16(key_len as u16)?; // key len 2 bytes

        let extra_len = if extra.is_some() { 4 } else { 0 };
        w.write_u8(extra_len)?; //extras length 1byte

        w.write_u8(0_u8)?; //data type 1byte

        w.write_u16(status as u16)?; // Status 2byte

        let response_len = response.as_ref().map_or(0, |r| r.len());
        let total_body_len = extra_len as u32 + key_len as u32 + response_len as u32;
        w.write_u32(total_body_len)?; // total body len: 4 bytes
        w.write_u32(0)?; //opaque: 4bytes
        w.write_u64(0)?; //cas: 8 bytes

        if let Some(extra) = extra {
            w.write_u32(extra)?;
        }
        if let Some(key) = &key {
            w.write_ringslice(key, 0)?;
        }
        if let Some(response) = response {
            w.write_slice(response, 0)? // value
        }
        Ok(())
    }

    #[inline]
    fn write_mc_response<W>(
        &self,
        request: &HashedCommand,
        response: Option<&crate::Command>,
        w: &mut W,
    ) -> crate::Result<()>
    where
        W: crate::Writer,
    {
        let old_op_code = request.op_code() as u8;

        let status = if response.is_some() && response.as_ref().unwrap().ok() {
            RespStatus::NoError
        } else {
            match old_op_code {
                OP_SET | OP_ADD => RespStatus::NotStored,
                OP_GET | OP_GETK | OP_DEL => RespStatus::NotFound,
                //对于所有的!rsp.ok都不返回，应该是只有not found不返回，其他错误返回
                OP_GETQ | OP_GETKQ => return Ok(()),
                _ => RespStatus::UnkownCmd,
            }
        };

        // flag涉及到不同语言的解析问题，需要考虑兼容，4096目前在java是bytearr
        const MARKER_BYTE_ARR: u32 = 4096u32;
        let (write_key, write_extra) = match old_op_code {
            OP_ADD | OP_SET | OP_DEL => {
                log::debug!("+++ OP_ADD write_mc_packet:{:?}", response);
                (None, None)
            }
            OP_GETK | OP_GETKQ => {
                let origin_req = request.origin_data();
                (Some(origin_req.key()), Some(MARKER_BYTE_ARR))
            }
            OP_GET | OP_GETQ => (None, Some(MARKER_BYTE_ARR)),
            _ => (None, None),
        };
        //协议与标准协议不一样了，add等也返回response了
        self.write_mc_packet(old_op_code, status, write_key, write_extra, response, w)?;
        Ok(())
    }

    /// 根据异常信息构建mc协议的error packet
    #[inline]
    fn build_error_rsp(&self, request: &RingSlice, emsg: &str) -> Vec<u8> {
        const PACKET_HEADER: usize = 24;
        let mut packet = Vec::with_capacity(PACKET_HEADER + emsg.len());
        packet.put_u8(RESPONSE_MAGIC); // magic
        packet.put_u8(request.op()); // opcode
        packet.put_u16(request.key_len()); // key len
        packet.put_u16(0); //extra_len + data_type + reserved
        packet.put_u16(RespStatus::InvalidArg as u16);
        let body_len = request.key_len() as u32 + emsg.len() as u32;
        packet.put_u32(body_len);
        packet.put_u32(request.opaque());
        packet.put_u64(0);

        if request.key_len() > 0 {
            request.key().copy_to_vec(&mut packet);
        }
        packet.extend(emsg.as_bytes());
        packet
    }
}

pub enum ConnState {
    ShakeHand,
    AuthSwitch,
    AuthMoreData,
    // 对于AuthError，通过直接返回异常来标志
    AuthOk,
}
