mod auth;
mod reqpacket;
mod rsppacket;

use super::Protocol;
use super::Result;
use crate::mysql::auth::native_auth;
use crate::Command;
use crate::Error;
use crate::HandShake;
use crate::RequestProcessor;
use crate::Stream;
use rsppacket::ResponsePacket;
use sharding::hash::Hash;

#[derive(Debug, Clone, Copy)]
pub(self) enum HandShakeStatus {
    #[allow(dead_code)]
    Init,
    InitialhHandshakeResponse,
    AuthSucceed,
}

#[derive(Clone, Default)]
pub struct Mysql;

impl Protocol for Mysql {
    //todo 握手
    fn handshake(
        &self,
        stream: &mut impl Stream,
        s: &mut impl crate::Writer,
        option: &mut crate::ResOption,
    ) -> Result<HandShake> {
        let mut packet = ResponsePacket::new(stream);
        match packet.ctx().status {
            HandShakeStatus::Init => match packet.take_initial_handshake() {
                Err(Error::ProtocolIncomplete) => Ok(HandShake::Continue),
                Ok(initial_handshake) => {
                    initial_handshake.check_fast_auth_and_native()?;
                    let response = packet.build_handshake_response(
                        option,
                        &native_auth(
                            initial_handshake.auth_plugin_data.as_bytes(),
                            option.token.as_bytes(),
                        ),
                    )?;
                    s.write(&response)?;

                    packet.ctx().status = HandShakeStatus::InitialhHandshakeResponse;
                    Ok(HandShake::Continue)
                }
                Err(e) => Err(e),
            },
            HandShakeStatus::InitialhHandshakeResponse => match packet.take_and_ok() {
                Ok(_) => {
                    packet.ctx().status = HandShakeStatus::AuthSucceed;
                    Ok(HandShake::Success)
                }
                Err(e) => Err(e),
            },
            HandShakeStatus::AuthSucceed => Ok(HandShake::Success),
        }
    }

    fn need_auth(&self) -> bool {
        true
    }

    // TODO in: mc vs redis, out: mysql
    fn parse_request<S: Stream, H: Hash, P: RequestProcessor>(
        &self,
        stream: &mut S,
        alg: &H,
        process: &mut P,
    ) -> Result<()> {
        Err(Error::ProtocolNotSupported)
    }

    // TODO mysql
    fn parse_response<S: crate::Stream>(&self, data: &mut S) -> Result<Option<Command>> {
        Err(Error::ProtocolNotSupported)
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
        Err(Error::Closed)
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

    //修改req，seq +1
    fn before_send<S: Stream, Req: crate::Request>(&self, _stream: &mut S, _req: &mut Req) {
        todo!()
    }
}
