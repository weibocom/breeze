// TODO 解析mysql协议， 转换为mc vs redis 协议
use ds::RingSlice;

use crate::{ResOption, Result};

use super::HandShakeStatus;
pub(super) struct InitialHandshake {
    pub(super) auth_plugin_name: String,
    pub(super) auth_plugin_data: String,
}

impl InitialHandshake {
    pub(super) fn check_fast_auth_and_native(&self) -> Result<()> {
        Ok(())
    }
}

// 这个context用于多请求之间的状态协作
// 必须是u64长度的。
#[repr(C)]
#[derive(Debug)]
pub(super) struct ResponseContext {
    pub(super) seq_id: u8,
    pub(super) status: HandShakeStatus,
    _ignore: [u8; 8],
}

impl From<&mut u64> for &mut ResponseContext {
    fn from(value: &mut u64) -> Self {
        unsafe { std::mem::transmute(value) }
    }
}

//解析rsp的时候，take的时候seq才加一？
pub(super) struct ResponsePacket<'a, S> {
    stream: &'a mut S,
    ctx: &'a mut ResponseContext,
}
impl<'a, S: crate::Stream> ResponsePacket<'a, S> {
    #[inline]
    pub(super) fn new(stream: &'a mut S) -> Self {
        let ctx = stream.context().into();
        Self { stream, ctx }
    }
    pub(super) fn ctx(&mut self) -> &mut ResponseContext {
        self.ctx
    }

    //解析initial_handshake，暂定解析成功会take走stream，协议未完成返回incomplete
    //如果这样会copy，可返回引用，外部take，但问题不大
    pub(super) fn take_initial_handshake(&mut self) -> Result<InitialHandshake> {
        todo!()
    }
    //构建采用Native Authentication快速认证的handshake response，seq+1
    pub(super) fn build_handshake_response(
        &mut self,
        option: &ResOption,
        auth_data: &[u8],
    ) -> Result<Vec<u8>> {
        todo!()
    }
    //take走一个packet，如果是err packet 返回错误类型，set+1
    pub(super) fn take_and_ok(&mut self) -> Result<()> {
        todo!();
    }
}
