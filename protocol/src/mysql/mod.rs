#[macro_use]
pub mod bitflags_ext;
mod auth;
pub mod constants;
mod error;
mod io;
pub mod mcpacket;
mod misc;
mod named_params;
mod opts;
mod packets;
mod params;
mod proto;
mod reqpacket;
mod row;
mod rsppacket;
mod scramble;
mod strategy;
mod value;

use self::mcpacket::PacketPos;
use self::mcpacket::RespStatus;
use self::mcpacket::*;
use self::opts::Opts;
use self::reqpacket::RequestPacket;
use self::rsppacket::ResponsePacket;
use self::strategy::MysqlStrategy;

use super::Flag;
use super::Protocol;
use super::Result;
use crate::mysql::mcpacket::QUITE_GET_TABLE;
use crate::Command;
use crate::Error;
use crate::HandShake;
use crate::HashedCommand;
use crate::RequestProcessor;
use crate::Stream;
use ds::MemGuard;
use mcpacket::Binary;
use sharding::hash::Hash;

#[derive(Clone)]
pub struct Mysql {
    convert_strategy: MysqlStrategy,
    // req_packet: RequestPacket,
}

impl Default for Mysql {
    fn default() -> Self {
        Mysql {
            convert_strategy: Default::default(),
            // req_packet: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(self) enum HandShakeStatus {
    #[allow(dead_code)]
    Init,
    InitialhHandshakeResponse,
    AuthSucceed,
}

impl Protocol for Mysql {
    fn handshake(
        &self,
        stream: &mut impl Stream,
        s: &mut impl crate::Writer,
        option: &mut crate::ResOption,
    ) -> Result<HandShake> {
        match self.handshake_inner(stream, s, option) {
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
        log::debug!("recv mysql-mc req:{:?}", stream.slice());

        // TODO: 这个需要把encode、decode抽出来，然后去掉mut，提升为Mysql结构体的字段 fishermen
        let mut req_packet = RequestPacket::new();

        // TODO request的解析部分待抽到reqpacket中
        while stream.len() >= mcpacket::HEADER_LEN {
            let mut req = stream.slice();
            req.check_request()?;
            let packet_len = req.packet_len();
            if req.len() < packet_len {
                stream.reserve(packet_len - req.len());
                break;
            }

            let last = !req.quiet_get(); // 须在map_op之前获取
            let cmd = req.operation();
            let op_code = req.map_op(); // 把quite get请求，转换成单个的get请求s
            let mut flag = Flag::from_op(op_code as u16, cmd);
            flag.set_try_next_type(req.try_next_type());
            flag.set_sentonly(req.sentonly());
            flag.set_noforward(req.noforward());

            let guard = stream.take(packet_len);
            let hash = req.hash(alg);
            let cmd = HashedCommand::new(guard, hash, flag);
            assert!(!cmd.data().quiet_get());
            process.process(cmd, last);

            // 确认请求类型和sql，构建mysql request
            // let sql = self.convert_strategy.build_sql(&req)?;
            // let mysql_cmd = req.mysql_cmd();
            // let request = req_packet.build_request(mysql_cmd, &sql)?;
        }

        Ok(())
    }

    fn build_request(&self, req: &mut HashedCommand, new_req: String) {
        let data = req.data();
        let mysql_cmd = data.mysql_cmd();
        let new_req = RequestPacket::new()
            .build_request(mysql_cmd, &new_req)
            .unwrap();
        *req.cmd() = MemGuard::from_vec(new_req);
    }

    // TODO in: mysql, out: mc vs redis
    //  1 解析mysql response； 2 转换为mc响应
    fn parse_response<S: crate::Stream>(&self, data: &mut S) -> Result<Option<Command>> {
        let mut rsp_packet = ResponsePacket::new(data, None);
        rsp_packet.proc_cmd()
    }

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
        let old_op_code = ctx.request().op_code();

        // 如果原始请求是quite_get请求，并且not found，则不回写。
        // if let Some(rsp) = ctx.response_mut() {
        if let Some(rsp) = response {
            assert!(rsp.data().len() > 0, "empty rsp:{:?}", rsp);

            // 验证Opaque是否相同. 不相同说明数据不一致
            if ctx.request().data().opaque() != rsp.data().opaque() {
                ctx.metric().inconsist(1);
            }

            // 先进行metrics统计
            //self.metrics(ctx.request(), Some(&rsp), ctx);

            // 如果quite 请求没拿到数据，直接忽略
            if QUITE_GET_TABLE[old_op_code as usize] == 1 && !rsp.ok() {
                return Ok(());
            }
            log::debug!("+++ will write mc rsp:{:?}", rsp.data());
            let data = rsp.data_mut();
            data.restore_op(old_op_code as u8);
            w.write_slice(data, 0)?;

            return Ok(());
        }

        // 先进行metrics统计
        //self.metrics(ctx.request(), None, ctx);

        match old_op_code as u8 {
            // noop: 第一个字节变更为Response，其他的与Request保持一致
            OP_CODE_NOOP => {
                w.write_u8(RESPONSE_MAGIC)?;
                w.write_slice(ctx.request().data(), 1)?;
            }

            //version: 返回固定rsp
            OP_CODE_VERSION => w.write(&VERSION_RESPONSE)?,

            // stat：返回固定rsp
            OP_CODE_STAT => w.write(&STAT_RESPONSE)?,

            // quit/quitq 无需返回rsp
            OP_CODE_QUIT | OP_CODE_QUITQ => return Err(Error::Quit),

            // quite get 请求，无需返回任何rsp，但没实际发送，rsp_ok设为false
            OP_CODE_GETQ | OP_CODE_GETKQ => return Ok(()),
            // 0x09 | 0x0d => return Ok(()),

            // set: mc status设为 Item Not Stored,status设为false
            OP_CODE_SET => {
                w.write(&self.build_empty_response(RespStatus::NotStored, ctx.request()))?
            }
            // self.build_empty_response(RespStatus::NotStored, req)

            // get/gets，返回key not found 对应的0x1
            OP_CODE_GET => {
                w.write(&self.build_empty_response(RespStatus::NotFound, ctx.request()))?
            }

            // self.build_empty_response(RespStatus::NotFound, req)

            // TODO：之前是直接mesh断连接，现在返回异常rsp，由client决定应对，观察副作用 fishermen
            _ => {
                log::warn!(
                    "+++ mc NoResponseFound req: {}/{:?}",
                    old_op_code,
                    ctx.request()
                );
                return Err(Error::OpCodeNotSupported(old_op_code));
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

impl Mysql {
    fn handshake_inner(
        &self,
        stream: &mut impl Stream,
        s: &mut impl crate::Writer,
        option: &mut crate::ResOption,
    ) -> Result<HandShake> {
        log::debug!("+++ recv mysql handshake packet:{:?}", stream.slice());
        let opt = Opts::from_user_pwd(option.username.clone(), option.token.clone());
        let mut packet = ResponsePacket::new(stream, Some(opt));

        match packet.ctx().status {
            HandShakeStatus::Init => {
                let handshake_rsp = packet.proc_handshake()?;
                s.write(&handshake_rsp)?;
                packet.ctx().status = HandShakeStatus::InitialhHandshakeResponse;
                Ok(HandShake::Continue)
            }
            HandShakeStatus::InitialhHandshakeResponse => {
                packet.proc_auth()?;
                packet.ctx().status = HandShakeStatus::AuthSucceed;
                Ok(HandShake::Success)
            }

            HandShakeStatus::AuthSucceed => Ok(HandShake::Success),
        }
    }

    // 根据req构建response，status为mc协议status，共11种
    #[inline]
    fn build_empty_response(&self, status: RespStatus, req: &HashedCommand) -> [u8; HEADER_LEN] {
        let req_slice = req.data();
        let mut response = [0; HEADER_LEN];
        response[PacketPos::Magic as usize] = RESPONSE_MAGIC;
        response[PacketPos::Opcode as usize] = req_slice.op();
        response[PacketPos::Status as usize + 1] = status as u8;
        //复制 Opaque
        for i in PacketPos::Opaque as usize..PacketPos::Opaque as usize + 4 {
            response[i] = req_slice.at(i);
        }
        response
    }

    //修改req，seq +1
    fn before_send<S: Stream, Req: crate::Request>(&self, _stream: &mut S, _req: &mut Req) {
        todo!()
    }
}

pub enum ConnState {
    ShakeHand,
    AuthSwitch,
    AuthMoreData,
    // 对于AuthError，通过直接返回异常来标志
    AuthOk,
}
