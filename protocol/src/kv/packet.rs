use std::fmt::Display;

use byteorder::{ByteOrder, LittleEndian};
use ds::RingSlice;

use crate::{Error, Result};

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

        // let payload_len = LittleEndian::read_u24(header_bytes) as usize;
        // self.payload_len = raw_chunk_len;
        // let seq = header_bytes[3];

        // TODO RingSlice 解析的标准用法，原则：最大限度减少copy，能否进一步优化？ fishermen
        let (payload_len, seq) = match self.inner.try_oneway_slice(*oft, HEADER_LEN) {
            Some(data) => {
                let len = LittleEndian::read_u24(data) as usize;
                if len == 0 {
                    log::warn!("+++ found slice 0-length packet: {:?}", data);
                }
                (LittleEndian::read_u24(data) as usize, data[3])
            }
            None => {
                let bytes = self.inner.dump_ring_part(*oft, HEADER_LEN);
                let len = LittleEndian::read_u24(bytes.as_slice()) as usize;
                if len == 0 {
                    log::warn!("+++ found ring packet 0-length packet: {:?}", bytes);
                }
                (LittleEndian::read_u24(&bytes) as usize, bytes[3])
            }
        };
        if payload_len == 0 {
            log::warn!(
                "+++ found 0-length packet:{:?}",
                self.inner.sub_slice(*oft, self.len() - *oft)
            );
        }

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
