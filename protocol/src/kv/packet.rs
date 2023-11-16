use std::fmt::Display;

use ds::{ByteOrder, RingSlice};

use super::error::{Error, Result};

const HEADER_LEN: usize = 4;

/// mysql的响应或请求packet的原始数据封装，packet data可能包含多个packet；
/// 如Com_Query的响应中包含一个定义columns的meta packet 和 若干个row packet。
#[derive(Debug)]
pub(super) struct PacketData {
    inner: RingSlice,
}

impl From<RingSlice> for PacketData {
    #[inline(always)]
    fn from(s: RingSlice) -> Self {
        Self { inner: s }
    }
}

/// 基于RingSlice解析mysql协议
impl PacketData {
    /// 解析packet header，
    pub(super) fn parse_header(&self, oft: &mut usize) -> Result<PacketHeader> {
        // mysql packet至少需要4个字节来读取sequence id
        if self.left_len(*oft) <= HEADER_LEN {
            return Err(Error::ProtocolIncomplete);
        }

        // 前三个字节是payload len
        let payload_len = self.inner.u24_le(*oft) as usize;
        if payload_len == 0 {
            log::warn!(
                "+++ found 0-length packet:{:?}",
                self.inner.sub_slice(*oft, self.len() - *oft)
            );
        }
        // 第四个字节是sequence id
        let seq = self.inner.at(*oft + 3);

        *oft = *oft + HEADER_LEN;
        Ok(PacketHeader { payload_len, seq })
    }

    #[inline(always)]
    pub(super) fn left_len(&self, oft: usize) -> usize {
        self.inner.len() - oft
    }
}

impl std::ops::Deref for PacketData {
    type Target = RingSlice;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Display for PacketData {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mysql:[{:?}]", self.inner)
    }
}

pub(super) struct PacketHeader {
    pub(super) payload_len: usize,
    pub(super) seq: u8,
}

impl PacketHeader {
    // #[inline(always)]
    // pub(super) fn packet_len(&self) -> usize {
    //     self.body_len + HEADER_LEN
    // }
}
