use ds::RingSlice;

use crate::Result;
use metrics::Path;

use super::{error::McqError, reqpacket::Packet};
use std::time::Duration;
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
                            if self.data.start_with(start, &"END\r".as_bytes())? {
                                self.rsp_type = RspType::End;
                            }
                        }
                        5 => {
                            if self.data.start_with(start, &"VALUE".as_bytes())? {
                                self.rsp_type = RspType::Value;
                            } else if self.data.start_with(start, &"ERROR".as_bytes())? {
                                self.rsp_type = RspType::Error;
                            }
                        }
                        6 => {
                            if self.data.start_with(start, &"STORED".as_bytes())? {
                                self.rsp_type = RspType::Stored;
                            }
                        }
                        7 => {
                            if self.data.start_with(start, &"DELETED".as_bytes())? {
                                self.rsp_type = RspType::Deleted;
                            }
                        }
                        9 => {
                            if self.data.start_with(start, &"NOT_FOUND".as_bytes())? {
                                self.rsp_type = RspType::NotFound;
                            }
                        }
                        10 => {
                            if self.data.start_with(start, &"NOT_STORED".as_bytes())? {
                                self.rsp_type = RspType::NotStored;
                            }
                        }
                        12 => {
                            if self.data.start_with(start, &"CLIENT_ERROR".as_bytes())? {
                                self.rsp_type = RspType::ClientError;
                            } else if self.data.start_with(start, &"SERVER_ERROR".as_bytes())? {
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
                        RspType::Unknown => return Err(McqError::RspInvalid.error()),
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
                            return Err(McqError::RspInvalid.error());
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
                            return Err(McqError::RspInvalid.error());
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
                        return Err(McqError::RspInvalid.error());
                    }
                }
                RspPacketState::RunToVal => {
                    if self.current() == LF {
                        state = RspPacketState::Val;
                    } else {
                        return Err(McqError::RspInvalid.error());
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
                        _ => return Err(McqError::RspInvalid.error()),
                    }
                }
                RspPacketState::ValLF => {
                    if self.current() == LF {
                        token = 0;
                        state = RspPacketState::End;
                    } else {
                        return Err(McqError::RspInvalid.error());
                    }
                }
                RspPacketState::End => {
                    assert!(token == 0);
                    if self.current() != b'E' {
                        return Err(McqError::RspInvalid.error());
                    }
                    // 当前只有VAL后的END这种场景，此时oft肯定大于0
                    token = self.oft;

                    self.data.token(&mut self.oft, 0)?;
                    if self.current() == CR {
                        let tlen = self.oft - token;
                        if tlen == 3 {
                            if self.data.start_with(token, &"END\r".as_bytes())? {
                                state = RspPacketState::AlmostDone;
                            }
                        } else {
                            return Err(McqError::RspInvalid.error());
                        }
                    } else if self.current() == b' ' {
                        // 目前不支持多个value
                        return Err(McqError::RspInvalid.error());
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
                    b' ' => break,
                    CR => state = RspPacketState::AlmostDone,
                    _ => return Err(McqError::RspInvalid.error()),
                },
                RspPacketState::AlmostDone => {
                    if self.current() == LF {
                        self.skip(1)?;
                        return Ok(());
                    }
                    return Err(McqError::RspInvalid.error());
                }
            }

            self.skip(1)?;
        }
        Err(super::Error::ProtocolIncomplete)
    }

    pub(super) fn delay_metric(&mut self) -> Result<()> {
        if !self.data.start_with(0, &"VALUE".as_bytes())? {
            return Ok(());
        };
        //todo 要严谨处理的话  还可以加高bit为1的判断
        if self.flags_len != 10 {
            return Ok(());
        }
        let time_sec = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut delay_metric = Path::base().rtt("mcq_delay");
        delay_metric += Duration::from_secs(time_sec - self.flags as u64);

        let mut i = 0;
        for idx in self.flags_start..(self.flags_start + self.flags_len) {
            if i == 0 {
                self.data.update(idx, b'0');
            } else {
                self.data.update(idx, b' ');
            }
            i += 1;
        }
        return Ok(());

        // let mut mem_data: Vec<u8> = Vec::with_capacity(rsp_mem.data().len());
        // rsp_mem.data().copy_to_vec(&mut mem_data);

        // if let Ok((key_start, _)) = next_token(&mem_data, 0) {
        //     if let Ok((flags_start, _key_len)) = next_token(&mem_data, key_start) {
        //         if let Ok((_vlen_start, flags_len)) = next_token(&mem_data, flags_start) {
        //             let flags_data = mem_data[flags_start..(flags_start + flags_len)].to_vec();

        //             let mut flags_value = 0;
        //             for flag in flags_data.iter() {
        //                 flags_value = flags_value * 10 + (flag - b'0') as usize;
        //             }
        //             // 认为是mesh set 时设置,
        //             if flags_len >= 10 {
        //                 let time_sec = SystemTime::now()
        //                     .duration_since(UNIX_EPOCH)
        //                     .unwrap()
        //                     .as_secs();

        //                 //let topic = mem_data[key_start..(key_start + key_len)].to_vec();
        //                 let mut delay_metric = Path::base().rtt("mcq_delay");
        //                 delay_metric += Duration::from_secs(time_sec - flags_value as u64);

        //                 let mut i = 0;
        //                 for idx in flags_start..(flags_start + flags_len) {
        //                     if i == 0 {
        //                         self.data.update(idx, b'0');
        //                     } else {
        //                         self.data.update(idx, b' ');
        //                     }
        //                     i += 1;
        //                 }
        //             }
        //         }
        //     }
        // }

        // Ok(())
    }

    pub(super) fn is_empty(&self) -> bool {
        self.rsp_type == RspType::End
    }

    #[inline]
    pub(super) fn take(&mut self) -> ds::MemGuard {
        assert!(self.oft_last < self.oft);
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
        Err(McqError::ReqInvalid.error())
    }

    #[inline]
    fn current(&self) -> u8 {
        assert!(self.available());
        self.data.at(self.oft)
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
