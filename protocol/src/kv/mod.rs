mod common;
mod mcpacket;
pub use mcpacket::Binary;

mod reqpacket;
mod rsppacket;

use self::common::proto::Text;
use self::common::query_result::QueryResult;
use self::common::row::convert::from_row;
use self::mcpacket::PacketPos;
use self::mcpacket::RespStatus;
use self::mcpacket::*;
use self::reqpacket::RequestPacket;
use self::rsppacket::ResponsePacket;

use super::Flag;
use super::Protocol;
use super::Result;
use crate::kv::common::opts::Opts;
use crate::Command;
use crate::Error;
use crate::HandShake;
use crate::HashedCommand;
use crate::RequestProcessor;
use crate::Stream;
use ds::MemGuard;

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
    ) -> Result<HandShake> {
        match self.handshake_inner(stream, option) {
            Ok(h) => Ok(h),
            Err(Error::ProtocolIncomplete) => Ok(HandShake::Continue),
            Err(e) => {
                log::warn!("+++ found err when shake hand:{:?}", e);
                Err(e)
            }
        }
    }

    fn need_auth(&self) -> bool {
        true
    }

    // 解析mc binary协议，在发送端进行协议转换
    fn parse_request<S: Stream, H: Hash, P: RequestProcessor>(
        &self,
        stream: &mut S,
        alg: &H,
        process: &mut P,
    ) -> Result<()> {
        assert!(stream.len() > 0, "mc req: {:?}", stream.slice());
        log::debug!("+++ recv mysql-mc req:{:?}", stream.slice());

        // TODO request的解析部分待抽到reqpacket中
        while stream.len() >= mcpacket::HEADER_LEN {
            let mut req = stream.slice();
            req.check_request()?;
            let packet_len = req.packet_len();
            if req.len() < packet_len {
                stream.reserve(packet_len - req.len());
                break;
            }

            if req.key_len() > 0 {
                log::debug!("+++ recv mysql-mc packet for key:{:?}", req.key());
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

    fn build_request(&self, req: &mut HashedCommand, new_req: String) {
        let mysql_cmd = req.mysql_cmd();
        let new_req = RequestPacket::new()
            .build_request(mysql_cmd, &new_req)
            .unwrap();
        // *req.cmd() = MemGuard::from_vec(new_req);
        req.reshape(MemGuard::from_vec(new_req));
    }

    // TODO in: mysql, out: mc vs redis
    //  1 解析mysql response； 2 转换为mc响应
    fn parse_response<S: crate::Stream>(&self, data: &mut S) -> Result<Option<Command>> {
        log::debug!("+++ recv mysql response:{:?}", data.slice());
        let mut rsp_packet = ResponsePacket::new(data, None);
        match self.parse_response_inner(&mut rsp_packet) {
            Ok(cmd) => Ok(cmd),
            Err(Error::ProtocolIncomplete) => {
                // rsp_packet.reserve();
                Ok(None)
            }
            Err(e) => {
                log::warn!("+++ err when parse mysql response: {:?}", e);
                Err(e)
            }
        }
    }

    // TODO 用于跟踪request和rsp解析过程的匹配，2023.7后清理 fishermen
    // fn parse_response_debug<S: crate::Stream>(
    //     &self,
    //     req: &HashedCommand,
    //     data: &mut S,
    // ) -> Result<Option<Command>> {
    //     log::debug!(
    //         "+++ recv mysql response for  req:{:?} =>{:?} with stream: {:?}",
    //         req.data(),
    //         data.slice(),
    //         data,
    //     );

    //     let mut rsp_packet = ResponsePacket::new(data, None);
    //     match self.parse_response_inner_debug(req, &mut rsp_packet) {
    //         Ok(cmd) => Ok(cmd),
    //         Err(Error::ProtocolIncomplete) => {
    //             // rsp_packet.reserve();
    //             Ok(None)
    //         }
    //         Err(e) => {
    //             log::warn!("+++ [req:{:?}] err when parse mysql response: {:?}", req, e);
    //             Err(e)
    //         }
    //     }
    // }

    fn write_response<C, W, M, I>(
        &self,
        ctx: &mut C,
        response: Option<&mut crate::Command>,
        w: &mut W,
    ) -> Result<()>
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

        // 如果原始请求是quite_get请求，并且not found，则不回写。
        let old_op_code = ctx.request().op_code() as u8;

        // 如果原始请求是quite_get请求，并且not found，则不回写。
        if let Some(rsp) = response {
            // mysql 请求到正确的数据，才会转换并write
            if rsp.ok() {
                assert!(rsp.len() > 0, "empty rsp:{:?}", rsp);
                self.write_mc_packet(ctx.request(), rsp, w)?;
                log::debug!(
                    "+++ sent to client for req:{:?}, rsp:{}",
                    ctx.request(),
                    rsp.len()
                );
                return Ok(());
            }
        }

        // 先进行metrics统计
        //self.metrics(ctx.request(), None, ctx);
        log::debug!("+++ send to client padding rsp, req:{:?}", ctx.request(),);
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
            OP_QUIT | OP_QUITQ => return Err(Error::Quit),

            // quite get 请求，无需返回任何rsp，但没实际发送，rsp_ok设为false
            OP_GETQ | OP_GETKQ => return Ok(()),
            // 0x09 | 0x0d => return Ok(()),

            // set: mc status设为 Item Not Stored,status设为false
            OP_SET => w.write(&self.build_empty_response(RespStatus::NotStored, ctx.request()))?,
            // self.build_empty_response(RespStatus::NotStored, req)

            // get/gets，返回key not found 对应的0x1
            OP_GET => w.write(&self.build_empty_response(RespStatus::NotFound, ctx.request()))?,

            // self.build_empty_response(RespStatus::NotFound, req)

            // TODO：之前是直接mesh断连接，现在返回异常rsp，由client决定应对，观察副作用 fishermen
            _ => {
                log::warn!(
                    "+++ mysql NoResponseFound req: {}/{:?}",
                    old_op_code,
                    ctx.request()
                );
                return Err(Error::OpCodeNotSupported(old_op_code as u16));
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

    fn check(&self, _req: &crate::HashedCommand, _resp: &crate::Command) {
        // TODO speed up
    }
}

impl Kv {
    fn handshake_inner(
        &self,
        stream: &mut impl Stream,
        option: &mut crate::ResOption,
    ) -> Result<HandShake> {
        log::debug!("+++ recv mysql handshake packet:{:?}", stream.slice());
        let opt = Opts::from_user_pwd(option.username.clone(), option.token.clone());
        let mut packet = ResponsePacket::new(stream, Some(opt));

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

    //修改req，seq +1
    // fn before_send<S: Stream, Req: crate::Request>(&self, _stream: &mut S, _req: &mut Req) {
    //     todo!()
    // }

    // 包比较复杂，不能随意take，得等到最复杂场景解析完毕后，才能统一take走
    // fn parse_response_inner_debug<'a, S: crate::Stream>(
    //     &self,
    //     req: &HashedCommand,
    //     rsp_packet: &'a mut ResponsePacket<'a, S>,
    // ) -> Result<Option<Command>> {
    //     // TODO: 是否需要把异常信息返回给client？（用mc协议？） fishermen
    //     let mut result_set = match self.parse_result_set(rsp_packet, Vec::new(), |mut acc, row| {
    //         acc.push(from_row(row));
    //         acc
    //     }) {
    //         Ok(rs) => rs,
    //         Err(Error::ProtocolIncomplete) => return Err(Error::ProtocolIncomplete),
    //         Err(Error::MysqlError) => Vec::with_capacity(0),
    //         Err(e) => panic!("mysql unkonw err: {:?}", e),
    //     };

    //     log::debug!(
    //         "+++ parsed rsp:{:?} for req req:{:?} ",
    //         req,
    //         result_set.len()
    //     );
    //     // 返回mysql响应，在write response处进行协议转换
    //     // TODO 这里临时打通，需要进一步完善修改 fishermen
    //     let mut row: Vec<u8> = match result_set.len() > 0 {
    //         true => result_set.remove(0),
    //         false => Vec::with_capacity(10),
    //     };

    //     let status = match row.len() {
    //         0 => false,
    //         _ => true,
    //     };

    //     let mem = MemGuard::from_vec(row);
    //     let cmd = Command::from(status, mem);
    //     Ok(Some(cmd))
    // }

    // mysql协议比较复杂，不能随意take，得等到最终解析完毕后，才能统一take走
    // TODO：后续可以统一记录解析位置及状态，从而减少重复解析 fishermen
    fn parse_response_inner<'a, S: crate::Stream>(
        &self,
        rsp_packet: &'a mut ResponsePacket<'a, S>,
    ) -> Result<Option<Command>> {
        let mut result_set = self.parse_result_set(rsp_packet)?;

        // 返回mysql响应，在write response处进行协议转换
        // TODO 这里临时打通，需要进一步完善修改 fishermen
        let status = result_set.len() > 0;

        let row: Vec<u8> = match result_set.len() > 0 {
            true => result_set.remove(0),
            false => {
                const NOT_FOUND: &[u8] = "not found".as_bytes();
                let mut padding = Vec::with_capacity(10);
                padding.extend(NOT_FOUND);
                padding
            }
        };

        let mem = MemGuard::from_vec(row);
        let cmd = Command::from(status, mem);
        Ok(Some(cmd))
    }

    fn parse_result_set<'a, S>(
        &self,
        rsp_packet: &'a mut ResponsePacket<'a, S>,
    ) -> Result<Vec<Vec<u8>>>
    where
        S: crate::Stream,
    {
        let meta = rsp_packet.parse_result_set_meta()?;
        let mut query_result: QueryResult<Text, S> = QueryResult::new(rsp_packet, meta);
        let collector = |mut acc: Vec<Vec<u8>>, row| {
            acc.push(from_row(row));
            acc
        };
        let result_set = query_result.scan_rows(Vec::with_capacity(4), collector)?;
        Ok(result_set)
    }

    #[inline]
    fn write_mc_packet<W>(
        &self,
        request: &HashedCommand,
        response: &crate::Command,
        w: &mut W,
    ) -> Result<()>
    where
        W: crate::Writer,
    {
        // header 24 bytes
        w.write_u8(mcpacket::Magic::Response as u8)?; //magic 1byte
        w.write_u8(request.op_code() as u8)?; // opcode 1 byte
        let origin_req = request.origin_data(); // old request
        let key_len = origin_req.key_len(); // old key len
        w.write_u16(origin_req.key_len())?; // key len 2 bytes
        let extra_len = 4_u8; // get 响应必须有extra，用于存放set时设置的flag
        w.write_u8(extra_len)?; //extras length 1byte
        w.write_u8(0_u8)?; //data type 1byte
        w.write_u16(mcpacket::RespStatus::NoError as u16)?; // Status 2byte
        let total_body_len = extra_len as u32 + key_len as u32 + response.len() as u32;
        w.write_u32(total_body_len)?; // total body len: 4 bytes
        w.write_u32(0)?; //opaque: 4bytes
        w.write_u64(0)?; //cas: 8 bytes

        // body： total body len
        // TODO flag涉及到不同语言的解析问题，需要考虑兼容 fishermen
        const FLAG_BYTEARR_JAVA: u32 = 4096;
        //extra, 只传bytearr类型（java为4096，其他语言如何处理？），由client解析
        w.write_u32(FLAG_BYTEARR_JAVA)?;

        // 返回key
        // TODO write_slice 实际是write的MemGuard，需要统一调整？ fishermen
        w.write_slice2(&origin_req.key(), 0)?;

        w.write_slice(response, 0) // value
    }
}

pub enum ConnState {
    ShakeHand,
    AuthSwitch,
    AuthMoreData,
    // 对于AuthError，通过直接返回异常来标志
    AuthOk,
}
