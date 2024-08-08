use ds::RingSlice;

use crate::{Result, Utf8};

use super::error::McqError;
const CR: u8 = 13;
const LF: u8 = 10;
const MAX_KEY_LEN: usize = 250;

pub(super) struct RspPacket<'a, S> {
    stream: &'a mut S,
    data: RingSlice,
    oft_last: usize,
    oft: usize,

    state: RspPacketState,
    rsp_type: RspType,

    token: usize,
    val_start: usize,
    vlen: usize,
    key_start: usize,
    key_len: usize,
    flags: usize,
    flags_len: usize,
    end: usize,
}

impl<'a, S: crate::Stream> RspPacket<'a, S> {
    pub(super) fn new(stream: &'a mut S) -> Self {
        let data = stream.slice();
        Self {
            stream,
            data,
            state: RspPacketState::Start,
            rsp_type: RspType::Unknown,
            token: 0,
            val_start: 0,
            vlen: 0,
            key_start: 0,
            key_len: 0,
            flags: 0,
            flags_len: 0,
            end: 0,
            oft_last: 0,
            oft: 0,
        }
    }

    // memcache rsponse 解析的状态机，后续考虑优化 fishermen
    pub(super) fn parse(&mut self) -> Result<()> {
        let mut first_loop = true;
        let mut state = self.state;

        if self.data.len() < 2 {
            return Err(super::Error::ProtocolIncomplete(0));
        }

        if self.current().is_ascii_digit() {
            state = RspPacketState::RspNum;
        } else {
            state = RspPacketState::RspStr;
        }

        let mut m;
        let mut p;
        while self.available() {
            match state {
                RspPacketState::Start => {
                    panic!("should already parsed some mcq rsp!");
                }
                RspPacketState::RspNum => {
                    if self.token == 0 {
                        self.token = self.oft;
                        self.val_start = self.oft;
                    }

                    if self.current().is_ascii_digit() {
                        // Do nothing
                    } else if self.current() == b' ' || self.current() == CR {
                        self.token = 0;
                        self.rsp_type = RspType::Num;
                        self.vlen = self.oft - self.val_start;
                        self.skip_back(1);
                        state = RspPacketState::CRLF;
                    } else {
                        return Err(McqError::ReqInvalid.error());
                    }
                }
                RspPacketState::RspStr => {
                    if self.token == 0 {
                        self.token = self.oft;
                    }
                    if self.current() == b' ' || self.current() == CR {
                        m = self.token;
                        self.token = 0;
                        self.rsp_type = RspType::Unknown;
                        let len = self.oft - m;
                        match len {
                            3 => {
                                if self.data.start_with(m, &"END\r".as_bytes())? {
                                    self.rsp_type = RspType::End;
                                    self.end = m;
                                }
                            }
                            5 => {
                                if self.data.start_with(m, &"VALUE".as_bytes())? {
                                    self.rsp_type = RspType::Value;
                                } else if self.data.start_with(m, &"ERROR".as_bytes())? {
                                    self.rsp_type = RspType::Error;
                                }
                            }
                            6 => {
                                if self.data.start_with(m, &"STORED".as_bytes())? {
                                    self.rsp_type = RspType::Stored;
                                } else if self.data.start_with(m, &"EXISTS".as_bytes())? {
                                    self.rsp_type = RspType::Exists;
                                }
                            }
                            7 => {
                                if self.data.start_with(m, &"DELETED".as_bytes())? {
                                    self.rsp_type = RspType::Deleted;
                                }
                            }
                            9 => {
                                if self.data.start_with(m, &"NOT_FOUND".as_bytes())? {
                                    self.rsp_type = RspType::NotFound;
                                }
                            }
                            10 => {
                                if self.data.start_with(m, &"NOT_STORED".as_bytes())? {
                                    self.rsp_type = RspType::NotStored;
                                }
                            }
                            12 => {
                                if self.data.start_with(m, &"CLIENT_ERROR".as_bytes())? {
                                    self.rsp_type = RspType::ClientError;
                                } else if self.data.start_with(m, &"SERVER_ERROR".as_bytes())? {
                                    self.rsp_type = RspType::ServerError;
                                }
                            }
                            _ => {
                                log::warn!("found malformed rsp: {:?}", self.data);
                                return Err(super::Error::ResponseProtocolInvalid);
                            }
                        }
                        match self.rsp_type {
                            RspType::Unknown => return Err(McqError::RspInvalid.error()),
                            RspType::Stored
                            | RspType::NotStored
                            | RspType::Exists
                            | RspType::NotFound
                            | RspType::Deleted => {
                                state = RspPacketState::CRLF;
                            }
                            RspType::End => {
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
                            _ => {
                                return Err(McqError::RspInvalid.error());
                            }
                        }
                        self.skip_back(1);
                    }
                }
                RspPacketState::SpacesBeforeKey => {
                    if self.current() != b' ' {
                        state = RspPacketState::Key;
                        self.skip_back(1);
                    }
                }
                RspPacketState::Key => {
                    if self.token == 0 {
                        self.token = self.oft;
                        self.key_start = self.oft;
                    }
                    if self.current() == b' ' {
                        self.key_len = self.oft - self.token;
                        if self.key_len > MAX_KEY_LEN {
                            return Err(McqError::RspInvalid.error());
                        }
                        self.token = 0;
                        state = RspPacketState::SpacesBeforeFlags;
                    }
                }
                RspPacketState::SpacesBeforeFlags => {
                    if self.current() != b' ' {
                        if !self.current().is_ascii_digit() {
                            return Err(McqError::RspInvalid.error());
                        }
                        state = RspPacketState::Flags;
                        self.skip_back(1);
                    }
                }
                RspPacketState::Flags => {
                    if self.token == 0 {
                        self.token = self.oft;
                        self.flags = self.oft;
                    }

                    if self.current().is_ascii_digit() {
                        // do nothing
                    } else if self.current() == b' ' {
                        self.flags_len = self.oft - self.flags;
                        self.token = 0;
                        state = RspPacketState::SpacesBeforeVlen;
                    } else {
                        return Err(McqError::RspInvalid.error());
                    }
                }
                RspPacketState::SpacesBeforeVlen => {
                    if self.current() != b' ' {
                        if !self.current().is_ascii_digit() {
                            return Err(McqError::RspInvalid.error());
                        }
                        self.skip_back(1);
                        state = RspPacketState::Vlen;
                    }
                }
                RspPacketState::Vlen => {
                    if self.token == 0 {
                        self.token = self.oft;
                        self.vlen = (self.current() - b'0') as usize;
                    } else if self.current().is_ascii_digit() {
                        self.vlen = self.vlen * 10 + (self.current() - b'0') as usize;
                    } else if self.current() == b' ' || self.current() == CR {
                        self.skip_back(1);
                        self.token = 0;
                        state = RspPacketState::RunToCRLF;
                    } else {
                        return Err(McqError::RspInvalid.error());
                    }
                }
                RspPacketState::RunToVal => {
                    if self.current() == LF {
                        self.val_start = self.oft + 1;
                        state = RspPacketState::Val;
                    } else {
                        return Err(McqError::RspInvalid.error());
                    }
                }
                RspPacketState::Val => {
                    m = self.oft + self.vlen;
                    // TODO 提升解析性能 speedup fishermen
                    if m >= self.data.len() {
                        return Err(super::Error::ProtocolIncomplete(0));
                    }
                    match self.data.at(m) {
                        CR => {
                            p = m;
                            state = RspPacketState::ValLF;
                        }
                        _ => return Err(McqError::RspInvalid.error()),
                    }
                }
                RspPacketState::ValLF => {
                    if self.current() == LF {
                        state = RspPacketState::End;
                    } else {
                        return Err(McqError::RspInvalid.error());
                    }
                }
                RspPacketState::End => {
                    if self.token == 0 {
                        if self.current() != b'E' || self.current() != b'V' {
                            return Err(McqError::RspInvalid.error());
                        }
                        self.token = self.oft;
                    } else if self.current() == CR {
                        m = self.token;
                        self.token = self.oft;

                        if (self.oft - m) == 3 {
                            if self.data.start_with(m, &"END\r".as_bytes())? {
                                self.end = m;
                                state = RspPacketState::AlmostDone;
                            }
                        } else {
                            return Err(McqError::RspInvalid.error());
                        }
                    } else if self.current() == b' ' {
                        // TODO 目前不支持多个value
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
                    b' ' => {
                        // do nothing just skip it
                    }
                    CR => state = RspPacketState::AlmostDone,
                    _ => return Err(McqError::RspInvalid.error()),
                },
                RspPacketState::AlmostDone => {
                    if self.current() == LF {
                        return Ok(());
                    }
                    return Err(McqError::RspInvalid.error());
                }
            }

            self.skip(1)?;
        }
        Err(super::Error::ProtocolIncomplete(0))
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
        return Err(super::Error::ProtocolIncomplete(0));
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
    Start,
    RspNum,
    RspStr,
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

enum RspType {
    Unknown,
    Num,
    Stored,
    NotStored,
    Exists,
    NotFound,
    End,
    Value,
    Deleted,
    Error,
    ClientError,
    ServerError,
}
