/// 用于解耦rsppacket 和 query_result，专门处理对同一个ringslice的多个mysql packet的解析
///
use std::{fmt::Display, num::NonZeroUsize};

use ds::{ByteOrder, RingSlice};

use crate::kv::{
    common::{
        constants::{CapabilityFlags, StatusFlags},
        error::Error::MySqlError,
        packets::{ErrPacket, OkPacket, OkPacketDeserializer, OkPacketKind},
        ParseBuf,
    },
    error::{Error, Result},
};

const HEADER_LEN: usize = 4;

/// mysql的响应或请求packet的原始数据封装，packet data可能包含多个packet；
/// 如Com_Query的响应中包含一个定义columns的meta packet 和 若干个row packet。
#[derive(Debug, Clone)]
pub struct MysqlRawPacket {
    inner: RingSlice,
    status_flags: StatusFlags,
    capability_flags: CapabilityFlags,
}

/// 基于RingSlice解析mysql协议
impl MysqlRawPacket {
    #[inline(always)]
    pub(super) fn new(
        s: RingSlice,
        status_flags: StatusFlags,
        capability_flags: CapabilityFlags,
    ) -> Self {
        Self {
            inner: s,
            status_flags,
            capability_flags,
        }
    }

    pub(super) fn handle_ok<T: OkPacketKind>(
        &mut self,
        payload: RingSlice,
    ) -> crate::kv::error::Result<OkPacket> {
        let ok = ParseBuf::from(payload)
            .parse::<OkPacketDeserializer<T>>(self.capability_flags)?
            .into_inner();
        // self.status_flags = ok.status_flags();
        Ok(ok)
    }

    // 尝试读下一个packet的payload，如果数据不完整，返回ProtocolIncomplete
    pub fn next_packet(&mut self, oft: &mut usize) -> Result<MysqlPacket> {
        let header = self.parse_header(oft)?;
        let sid;
        match NonZeroUsize::new(header.payload_len) {
            Some(_chunk_len) => {
                // 当前请求基于com query方式，不需要seq id fishermen
                // self.codec.set_seq_id(header.seq.wrapping_add(1));
                sid = header.seq;
            }
            None => {
                // 当前mysql协议，不应该存在payload长度为0的packet fishermen
                let emsg: Vec<u8> = "zero len response".as_bytes().to_vec();
                log::error!("malformed mysql rsp: {}/{}", oft, self);
                return Err(Error::UnhandleResponseError(emsg));
            }
        };

        // 4 字节之后是packet 的 body/payload 以及其他的packet
        let left_len = self.left_len(*oft);
        if left_len >= header.payload_len {
            // 0xFF ERR packet header
            if self.at(*oft) == 0xFF {
                // self.oft += header.payload_len;
                match ParseBuf::from(self.sub_slice(*oft, left_len)).parse(self.capability_flags)? {
                    // server返回的异常响应转为MysqlError，对于client request，最终传给client，不用断连接；
                    // 对于连接初期的auth，则直接断连接即可；
                    ErrPacket::Error(server_error) => {
                        // self.handle_err();
                        *oft += header.payload_len;
                        log::warn!("+++ parse packet err:{:?}", server_error);
                        return Err(MySqlError(From::from(server_error)).error());
                    }
                    ErrPacket::Progress(_progress_report) => {
                        *oft += header.payload_len;
                        log::error!("+++ parse packet Progress err:{:?}", _progress_report);
                        // 暂时先不支持诸如processlist、session等指令，所以不应该遇到progress report 包 fishermen
                        //return self.next_packet();
                        panic!("unsupport progress report: {}/{:?}", *oft, self);
                    }
                }
            }

            // self.seq_id = self.seq_id.wrapping_add(1);

            let payload_start = *oft;
            *oft += header.payload_len;

            return Ok(MysqlPacket {
                seq: sid,
                payload: self.sub_slice(payload_start, header.payload_len),
            });
        }

        // 数据没有读完，reserve可读取空间，返回Incomplete异常
        // self.reserve();
        Err(Error::ProtocolIncomplete)
    }

    /// 解析packet header，
    fn parse_header(&self, oft: &mut usize) -> Result<MysqlPacketHeader> {
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
        Ok(MysqlPacketHeader { payload_len, seq })
    }

    #[inline(always)]
    pub fn left_len(&self, oft: usize) -> usize {
        self.inner.len() - oft
    }

    /// Must not be called before handle_handshake.
    pub(super) const fn has_capability(&self, flag: CapabilityFlags) -> bool {
        self.capability_flags.contains(flag)
    }

    pub(super) fn more_results_exists(&self) -> bool {
        self.status_flags
            .contains(StatusFlags::SERVER_MORE_RESULTS_EXISTS)
    }
}

impl std::ops::Deref for MysqlRawPacket {
    type Target = RingSlice;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Display for MysqlRawPacket {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "mysql packet:[{:?}], status:{:?}, capability:{:?}",
            self.inner, self.status_flags, self.capability_flags
        )
    }
}

struct MysqlPacketHeader {
    pub payload_len: usize,
    pub seq: u8,
}

impl MysqlPacketHeader {
    // #[inline(always)]
    // pub(super) fn packet_len(&self) -> usize {
    //     self.body_len + HEADER_LEN
    // }
}

/// 解析后的mysql packet
#[derive(Debug)]
pub struct MysqlPacket {
    pub seq: u8,
    pub payload: RingSlice,
}

impl Display for MysqlPacket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "seq: {}, payload:{:?}", self.seq, self.payload)
    }
}
