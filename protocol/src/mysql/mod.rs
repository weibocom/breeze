mod reqpacket;
mod rsppacket;

use super::Protocol;
use super::Result;
use crate::Command;
use crate::Error;
use crate::RequestProcessor;
use crate::Stream;
use sharding::hash::Hash;

#[derive(Debug)]
pub(self) enum HandShakeStatus {
    Init,
}

#[derive(Clone, Default)]
pub struct Mysql;

impl Protocol for Mysql {
    //todo 握手
    fn handshake(
        &self,
        _stream: &mut impl Stream,
        s: &mut impl crate::Writer,
        option: &mut crate::ResOption,
    ) -> Result<crate::HandShake> {
        todo!();
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
