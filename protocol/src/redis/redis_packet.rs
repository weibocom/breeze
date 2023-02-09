use super::packet::*;
use crate::Result;

#[repr(C)]
#[derive(Debug)]
pub struct RedisRequestContext {
    bulk: u16,
    op_code: u16,
    first: bool,
    layer: u8, // 请求的层次，目前只支持：master，all
    _ignore: [u8; 2],
}

// 请求的layer层次，目前只有masterOnly，后续支持业务访问某层时，在此扩展属性
pub enum LayerType {
    MasterOnly = 1,
}

impl RedisRequestContext {
    #[inline]
    fn from_packet(v: &RequestContext) -> &Self {
        unsafe { std::mem::transmute(v) }
    }
    #[inline]
    fn from_packet_mut(v: &mut RequestContext) -> &mut Self {
        unsafe { std::mem::transmute(v) }
    }
}

// impl From<&mut RequestContext> for &mut RedisRequestContext {
//     fn from(value: &mut RequestContext) -> Self {
//         unsafe { std::mem::transmute(value) }
//     }
// }

pub(super) struct RedisRequestPacket<'a, S> {
    req_packet: RequestPacket<'a, S>,
    reserved_hash: i64,
}

use std::ops::{Deref, DerefMut};
impl<'a, S: crate::Stream> Deref for RedisRequestPacket<'a, S> {
    type Target = RequestPacket<'a, S>;
    fn deref(&self) -> &Self::Target {
        &self.req_packet
    }
}
impl<'a, S: crate::Stream> DerefMut for RedisRequestPacket<'a, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.req_packet
    }
}

impl<'a, S: crate::Stream> RedisRequestPacket<'a, S> {
    #[inline]
    pub(super) fn new(stream: &'a mut S) -> Self {
        let reserved_hash = *stream.reserved_hash();
        let req_packet = RequestPacket::new(stream);
        Self {
            req_packet,
            reserved_hash,
        }
    }

    // 重置reserved hash，包括stream中的对应值
    #[inline]
    fn reset_reserved_hash(&mut self) {
        self.update_reserved_hash(0)
    }
    // 更新reserved hash
    #[inline]
    pub(super) fn update_reserved_hash(&mut self, reserved_hash: i64) {
        self.reserved_hash = reserved_hash;
        *self.stream().reserved_hash() = reserved_hash;
    }

    #[inline]
    pub(super) fn reserved_hash(&self) -> i64 {
        self.reserved_hash
    }

    #[inline]
    pub(super) fn take(&mut self) -> ds::MemGuard {
        let data = self.req_packet.take();
        if self.bulk() == 0 {
            self.reset_reserved_hash();
        }
        data
    }

    // trim掉已解析的cmd相关元数据，只保留在master_only、reserved_hash这两个元数据
    #[inline]
    pub(super) fn trim_swallowed_cmd(&mut self) -> Result<()> {
        // trim 吞噬指令时，整个吞噬指令必须已经被解析完毕
        debug_assert!(self.bulk() == 0, "packet:{}", self);

        // 记录需保留的状态：目前只有master only状态【direct hash保存在stream的非ctx字段中】
        let master_only = self.master_only();

        //重置ctx
        let _ = self.take();

        // 保留后续cmd执行需要的状态：当前只有master
        if master_only {
            // 保留master only 设置
            self.set_layer(LayerType::MasterOnly);
        }

        // 设置packet的ctx到stream的ctx中，供下一个指令使用
        self.reserver_context();

        if self.available() {
            return Ok(());
        }
        return Err(crate::Error::ProtocolIncomplete);
    }

    #[inline]
    pub(super) fn set_layer(&mut self, layer: LayerType) {
        RedisRequestContext::from_packet_mut(self.ctx_mut()).layer = layer as u8;
    }
    #[inline]
    pub(super) fn master_only(&self) -> bool {
        RedisRequestContext::from_packet(self.ctx()).layer == LayerType::MasterOnly as u8
    }
}

use std::fmt::{self, Debug, Display, Formatter};

impl<'a, S: crate::Stream> Display for RedisRequestPacket<'a, S> {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.req_packet, f)
    }
}
impl<'a, S: crate::Stream> Debug for RedisRequestPacket<'a, S> {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}
