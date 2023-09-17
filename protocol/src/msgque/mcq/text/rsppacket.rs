use ds::RingSlice;

use crate::Result;
use metrics::Path;

use super::{error::McqError, reqpacket::Packet};
use ds::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
const CR: u8 = 13;
const LF: u8 = 10;
const MAX_KEY_LEN: usize = 250;

pub(super) struct RspPacket<'a, S> {
    stream: &'a mut S,
    data: RingSlice,
    oft_last: usize,
    oft: usize,
    rsp_type: RspType,
    // TODO： 先保留，等测试完毕后再清理
    // state: RspPacketState,
    // token: usize,
    // val_start: usize,
    // vlen: usize,
    // key_start: usize,
    // key_len: usize,
    flags: usize,
    flags_len: usize,
    flags_start: usize,
    // end: usize,
}

impl<'a, S: crate::Stream> RspPacket<'a, S> {
    pub(super) fn new(stream: &'a mut S) -> Self {
        let data = stream.slice();
        Self {
            stream,
            data,
            oft_last: 0,
            oft: 0,
            rsp_type: RspType::Unknown,
            // TODO 先保留
            // state: RspPacketState::Start,

            // token: 0,
            // val_start: 0,
            // vlen: 0,
            // key_start: 0,
            // key_len: 0,
            flags: 0,
            flags_len: 0,
            flags_start: 0,
            // end: 0,
        }
    }
    #[inline]
    fn start_with(&self, oft: usize, s: &[u8]) -> Result<bool> {
        if oft + s.len() > self.data.len() {
            Err(crate::Error::ProtocolIncomplete)
        } else {
            Ok(self.data.start_with(oft, s))
        }
    }

    // memcache rsponse 解析的状态机，后续考虑优化 fishermen
    pub(super) fn parse(&mut self) -> Result<()> {
        if self.data.len() < 2 {
            return Err(super::Error::ProtocolIncomplete);
        }

        let mut state = RspPacketState::RspStr;
        let mut token = 0;
        let mut vlen = 0;
        while self.available() {
            match state {
                RspPacketState::RspStr => {
                    token = self.oft;
                    self.data.token(&mut self.oft, 0)?;
                    let tlen = self.oft - token;
                    let start = token;
                    token = self.oft;
                    self.rsp_type = RspType::Unknown;
                    match tlen {
                        3 => {
                            if self.start_with(start, &"END\r".as_bytes())? {
                                self.rsp_type = RspType::End;
                            }
                        }
                        5 => {
                            if self.start_with(start, &"VALUE".as_bytes())? {
                                self.rsp_type = RspType::Value;
                            } else if self.start_with(start, &"ERROR".as_bytes())? {
                                self.rsp_type = RspType::Error;
                            }
                        }
                        6 => {
                            if self.start_with(start, &"STORED".as_bytes())? {
                                self.rsp_type = RspType::Stored;
                            }
                        }
                        7 => {
                            if self.start_with(start, &"DELETED".as_bytes())? {
                                self.rsp_type = RspType::Deleted;
                            }
                        }
                        9 => {
                            if self.start_with(start, &"NOT_FOUND".as_bytes())? {
                                self.rsp_type = RspType::NotFound;
                            }
                        }
                        10 => {
                            if self.start_with(start, &"NOT_STORED".as_bytes())? {
                                self.rsp_type = RspType::NotStored;
                            }
                        }
                        12 => {
                            if self.start_with(start, &"CLIENT_ERROR".as_bytes())? {
                                self.rsp_type = RspType::ClientError;
                            } else if self.start_with(start, &"SERVER_ERROR".as_bytes())? {
                                self.rsp_type = RspType::ServerError;
                            }
                        }
                        _ => {
                            log::warn!("found malformed rsp: {:?}", self.data);
                            return Err(super::Error::ResponseProtocolInvalid);
                        }
                    }

                    match self.rsp_type {
                        RspType::Stored
                        | RspType::NotStored
                        | RspType::NotFound
                        | RspType::Deleted
                        | RspType::End => {
                            state = RspPacketState::CRLF;
                        }
                        RspType::Value => {
                            state = RspPacketState::SpacesBeforeKey;
                        }
                        RspType::Error => {
                            state = RspPacketState::CRLF;
                        }
                        RspType::ClientError | RspType::ServerError => {
                            state = RspPacketState::RunToCRLF;
                        }
                        RspType::Unknown => return Err(McqError::RspInvalid.into()),
                    }
                    self.skip_back(1)?;
                }
                RspPacketState::SpacesBeforeKey => {
                    if self.current() != b' ' {
                        state = RspPacketState::Key;
                        self.skip_back(1)?;
                    }
                }
                RspPacketState::Key => {
                    self.data.token(&mut self.oft, MAX_KEY_LEN)?;
                    if self.current() == b' ' {
                        state = RspPacketState::SpacesBeforeFlags;
                    }
                }
                RspPacketState::SpacesBeforeFlags => {
                    if self.current() != b' ' {
                        if !self.current().is_ascii_digit() {
                            return Err(McqError::RspInvalid.into());
                        }
                        state = RspPacketState::Flags;
                        self.skip_back(1)?;
                    }
                }
                RspPacketState::Flags => {
                    if self.current() != b' ' {
                        if self.flags_start == 0 {
                            self.flags_start = self.oft;
                        }
                        self.flags = self.flags * 10 + (self.current() - b'0') as usize;
                        self.flags_len += 1;
                    } else {
                        self.skip_back(1)?;
                        self.data.token(&mut self.oft, 0)?;
                        state = RspPacketState::SpacesBeforeVlen;
                    }
                }
                RspPacketState::SpacesBeforeVlen => {
                    if self.current() != b' ' {
                        if !self.current().is_ascii_digit() {
                            return Err(McqError::RspInvalid.into());
                        }
                        self.skip_back(1)?;
                        state = RspPacketState::Vlen;
                    }
                }
                RspPacketState::Vlen => {
                    if self.current().is_ascii_digit() {
                        vlen = vlen * 10 + (self.current() - b'0') as usize;
                    } else if self.current() == b' ' || self.current() == CR {
                        self.skip_back(1)?;
                        state = RspPacketState::RunToCRLF;
                    } else {
                        return Err(McqError::RspInvalid.into());
                    }
                }
                RspPacketState::RunToVal => {
                    if self.current() == LF {
                        state = RspPacketState::Val;
                    } else {
                        return Err(McqError::RspInvalid.into());
                    }
                }
                RspPacketState::Val => {
                    token = self.oft + vlen;
                    if token >= self.data.len() {
                        return Err(super::Error::ProtocolIncomplete);
                    }
                    self.skip(vlen)?;
                    match self.current() {
                        CR => {
                            state = RspPacketState::ValLF;
                        }
                        _ => return Err(McqError::RspInvalid.into()),
                    }
                }
                RspPacketState::ValLF => {
                    if self.current() == LF {
                        token = 0;
                        state = RspPacketState::End;
                    } else {
                        return Err(McqError::RspInvalid.into());
                    }
                }
                RspPacketState::End => {
                    assert!(token == 0, "token:{}, rsp:{:?}", token, self.data);
                    if self.current() != b'E' {
                        return Err(McqError::RspInvalid.into());
                    }
                    // 当前只有VAL后的END这种场景，此时oft肯定大于0
                    token = self.oft;

                    self.data.token(&mut self.oft, 0)?;
                    if self.current() == CR {
                        let tlen = self.oft - token;
                        if tlen == 3 {
                            if self.start_with(token, &"END\r".as_bytes())? {
                                state = RspPacketState::AlmostDone;
                            }
                        } else {
                            return Err(McqError::RspInvalid.into());
                        }
                    } else if self.current() == b' ' {
                        // 目前不支持多个value
                        return Err(McqError::RspInvalid.into());
                    }
                }
                RspPacketState::RunToCRLF => {
                    if self.current() == CR {
                        match self.rsp_type {
                            RspType::Value => state = RspPacketState::RunToVal,
                            _ => state = RspPacketState::AlmostDone,
                        }
                    }
                }
                RspPacketState::CRLF => match self.current() {
                    b' ' => {
                        // do nothing, just skip
                    }
                    CR => state = RspPacketState::AlmostDone,
                    _ => return Err(McqError::RspInvalid.into()),
                },
                RspPacketState::AlmostDone => {
                    if self.current() == LF {
                        self.skip(1)?;
                        return Ok(());
                    }
                    return Err(McqError::RspInvalid.into());
                }
            }

            self.skip(1)?;
        }
        Err(super::Error::ProtocolIncomplete)
    }

    pub(super) fn delay_metric(&mut self) -> Result<()> {
        // 只处理value类型的rsp（request为get xxx）
        if self.rsp_type != RspType::Value {
            return Ok(());
        }

        // 业务层只有接少数场景会用到flags 字段，也就是绝大多数场景下为0
        // 即使使用也为u8类型，实际场景下不会存在置为时间戳的场景, 万一使用，则存在被覆盖的bug
        //
        // 超过一个小时(3600s) ,延时指标统计也无实际意义
        let time_sec = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        const HOUR_SECS: u64 = 3600;
        let sub = time_sec - self.flags as u64;
        if sub > HOUR_SECS {
            return Ok(());
        }

        let mut delay_metric = Path::base().rtt("mcq_delay");
        delay_metric += Duration::from_secs(sub);

        for idx in self.flags_start..(self.flags_start + self.flags_len) {
            self.data.update(idx, b'0');
        }
        return Ok(());
    }

    #[inline]
    pub(super) fn take(&mut self) -> ds::MemGuard {
        assert!(
            self.oft_last < self.oft,
            "oft: {}/{}, rsp:{:?}",
            self.oft_last,
            self.oft,
            self.data
        );
        let data = self.data.sub_slice(self.oft_last, self.oft - self.oft_last);
        self.oft_last = self.oft;

        self.stream.take(data.len())
    }

    #[inline]
    pub(super) fn available(&self) -> bool {
        self.oft < self.data.len()
    }

    #[inline]
    fn skip(&mut self, count: usize) -> Result<()> {
        self.oft += count;
        if self.oft <= self.data.len() {
            return Ok(());
        }
        return Err(super::Error::ProtocolIncomplete);
    }

    #[inline]
    fn skip_back(&mut self, count: usize) -> Result<()> {
        self.oft -= count;
        if self.oft >= self.oft_last {
            return Ok(());
        }
        Err(McqError::ReqInvalid.into())
    }

    #[inline]
    fn current(&self) -> u8 {
        assert!(self.available(), "oft:{}, rsp:{:?}", self.oft, self.data);
        self.data.at(self.oft)
    }

    // succeed 标准： get 返回val；set 返回 stored；
    #[inline]
    pub(crate) fn is_succeed(&self) -> bool {
        match self.rsp_type {
            RspType::Value | RspType::Stored => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum RspPacketState {
    // Start,
    // RspNum,
    RspStr, // 区分mc协议的数字响应
    SpacesBeforeKey,
    Key,

    SpacesBeforeFlags,
    Flags,
    SpacesBeforeVlen,
    Vlen,
    RunToVal,
    Val,
    ValLF,
    End,
    RunToCRLF,
    CRLF,
    AlmostDone,
}

#[derive(PartialEq, Eq)]
enum RspType {
    Unknown,
    Stored,
    NotStored,
    NotFound, // delete 的key不存在
    End,
    Value,
    Deleted,
    Error,
    ClientError,
    ServerError,
    // Num,
    // Exists,  // cas rsp，mcq目前不需要
}
