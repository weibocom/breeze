use ds::RingSlice;
use lazy_static::__Deref;
use sharding::hash::{Bkdr, Hash};

use crate::{msgque::mcq::text::reqpacket, OpCode, Result, Utf8};

use super::{
    command::{self, CommandProperties, RequestType},
    error::McqError,
};

const CR: u8 = 13;
const LF: u8 = 10;
const MAX_KEY_LEN: usize = 250;

pub(super) struct RequestPacket<'a, S> {
    // 生命周期为解析多个req
    stream: &'a mut S,
    data: RingSlice,
    oft_last: usize,
    oft: usize,

    // 生命周期为开始解析当前req，到下一个req解析开始
    op_code: OpCode, // u16 commdProps的idx
    cmd_cfg: Option<&'a CommandProperties>,
    // TODO 这些字段暂时保留，确认不需要后，清理 fishermen
    state: ReqPacketState,
    key_start: usize,
    key_len: usize,
    token: usize,
    flags: usize,
    flen: usize,
    vlen: usize,
    real_vlen: usize,
    unique_id: usize,
    unique_id_len: usize,
    val_start: usize,
    noreply: bool,
}

impl<'a, S: crate::Stream> RequestPacket<'a, S> {
    #[inline]
    pub fn new(stream: &'a mut S) -> Self {
        let data = stream.slice();
        Self {
            stream,
            data,
            oft_last: 0,
            oft: 0,
            op_code: 0, // 即Commands中cmd props的idx
            cmd_cfg: None,
            // 有需要再打开，验证完毕后再清理
            state: ReqPacketState::Start,
            key_start: 0,
            key_len: 0,
            token: 0,
            flags: 0,
            flen: 0,
            vlen: 0,
            real_vlen: 0,
            unique_id: 0,
            unique_id_len: 0,
            val_start: 0,
            noreply: false,
            // msg_type: MsgType::Unknown,
        }
    }

    #[inline]
    pub(super) fn available(&self) -> bool {
        self.oft < self.data.len()
    }

    #[inline]
    pub(super) fn op_code(&self) -> OpCode {
        self.op_code
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

    // #[inline]
    // pub(super) fn state(&self) -> &ReqPacketState {
    //     &self.op_code
    // }

    #[inline]
    fn current(&self) -> u8 {
        assert!(self.available());
        self.data.at(self.oft)
    }

    #[inline]
    pub fn cmd_cfg(&self) -> &CommandProperties {
        if let None = self.cmd_cfg {
            log::error!("mcq cmd cfg should not be None here!");
            panic!("mcq cmd cfg should not be None here!");
        }
        self.cmd_cfg.unwrap()
    }

    // TODO:
    #[inline]
    pub fn prepare_for_parse(&mut self) {
        if self.available() {}
    }

    // TODO：简化 memcache 状态机，去掉对非mcq指令的字段解析
    pub(super) fn parse_req(&mut self) -> Result<()> {
        let mut state = ReqPacketState::Start;
        let mut first_loop = true;
        let mut m;
        let mut token = self.oft;; // 记录某个token，like key、flag等
        let mut vlen = self.oft;;  // value len
        // let mut unique_id;

        while self.available() {
            if first_loop {
                first_loop = false;
            } else {
                self.skip(1)?;
            }

            match state {
                ReqPacketState::Start => {
                    if self.current() == b' ' {
                        break;
                    }
                    if !self.current().is_ascii_lowercase() {
                        return Err(McqError::ReqInvalid.error());
                    }

                    token = self.oft;
                    state = ReqPacketState::ReqType;
                }
                ReqPacketState::ReqType => {
                    if self.current() == b' ' || self.current() == CR {
                        // token = self.token;
                        // self.token = 0;

                        let cmd_len = self.oft - token;
                        let cmd_name = self.data.sub_slice(token, cmd_len);
                        let cfg = command::get_cfg(command::get_op_code(&cmd_name))?;
                        self.op_code = cfg.op_code();
                        let req_type = cfg.req_type();
                        self.cmd_cfg = Some(cfg);

                        // 对非单行指令过滤
                        match req_type {
                            RequestType::Get | RequestType::Set | RequestType::Delete => {
                                if self.current() == CR {
                                    return Err(McqError::ReqInvalid.error());
                                }
                                state = ReqPacketState::SpacesBeforeKey;
                            }
                            RequestType::Quit => {
                                self.skip_back(1)?;
                                state = ReqPacketState::CRLF;
                            }
                            RequestType::Stats => {
                                if self.current() == CR {
                                    self.skip_back(1)?;
                                    state = ReqPacketState::CRLF;
                                } else if self.current() == b' ' {
                                    state = ReqPacketState::SpacesBeforeKey;
                                }
                            }
                            _ => {
                                return Err(McqError::ReqInvalid.error());
                            }
                        }
                    } else if !self.current().is_ascii_lowercase() {
                        return Err(McqError::ReqInvalid.error());
                    }
                }
                ReqPacketState::SpacesBeforeKey => {
                    if self.current() != b' ' {
                        token = self.oft;
                        // key_start = self.oft;
                        state = ReqPacketState::Key;
                    }
                }
                ReqPacketState::Key => {
                    if self.current() == b' ' || self.current() == CR {
                        let len = self.oft - token;
                        if len > MAX_KEY_LEN {
                            return Err(McqError::ReqInvalid.error());
                        }
                        // self.key_len = len;
                        // self.token = 0;
                        token = 0;

                        let req_type = self.cmd_cfg().req_type();
                        if req_type.is_storage() {
                            state = ReqPacketState::SpacesBeforeFlags;
                        } else if req_type.is_delete() {
                            state = ReqPacketState::RunToCRLF;
                        } else if req_type.is_retrieval() {
                            state = ReqPacketState::SpacesBeforeKeys;
                        } else {
                            state = ReqPacketState::RunToCRLF;
                        }

                        if self.current() == CR {
                            if req_type.is_storage() {
                                return Err(McqError::ReqInvalid.error());
                            }
                            // 回退1byte，方便后面解析CRLF
                            self.skip_back(1)?;
                        }
                    }
                }
                ReqPacketState::SpacesBeforeKeys => {
                    match self.current() {
                        b' ' => break,
                        CR => state = ReqPacketState::AlmostDone,
                        _ => {
                            // mcq 目前不支持多key
                            return Err(McqError::ReqInvalid.error());
                        }
                    }
                }
                ReqPacketState::SpacesBeforeFlags => {
                    if self.current() != b' ' {
                        if !self.current().is_ascii_digit() {
                            return Err(McqError::ReqInvalid.error());
                        }
                        token = self.oft;
                        // self.flags = self.oft;
                        state = ReqPacketState::Flags;
                    }
                }
                ReqPacketState::Flags => {
                    if self.current().is_ascii_digit() {
                        break;
                    } else if self.current() == b' ' {
                        // self.flen = self.oft - self.flags;
                        // self.token = 0;
                        token = 0;
                        state = ReqPacketState::SpacesBeforeExpire;
                    } else {
                        return Err(McqError::ReqInvalid.error());
                    }
                }
                ReqPacketState::SpacesBeforeExpire => {
                    if self.current() != b' ' {
                        if !self.current().is_ascii_digit() {
                            return Err(McqError::ReqInvalid.error());
                        }
                        token = self.oft;
                        state = ReqPacketState::Expire;
                    }
                }
                ReqPacketState::Expire => {
                    if self.current().is_ascii_digit() {
                        break;
                    } else if self.current() == b' ' {
                        token = 0;
                        state = ReqPacketState::SpacesBeforeVlen;
                    } else {
                        return Err(McqError::ReqInvalid.error());
                    }
                }
                ReqPacketState::SpacesBeforeVlen => {
                    if self.current() != b' ' {
                        if !self.current().is_ascii_digit() {
                            return Err(McqError::ReqInvalid.error());
                        }
                        token = self.oft;
                        vlen = (self.current() - b'0') as usize;
                        state = ReqPacketState::Vlen;
                    }
                }
                ReqPacketState::Vlen => {
                    if self.current().is_ascii_digit() {
                        vlen = vlen * 10 + (self.current() - b'0') as usize;
                    } else if self.current() == b' ' || self.current() == CR {
                        // self.real_vlen = self.vlen;
                        self.skip_back(1)?;
                        token = 0;
                        state = ReqPacketState::RunToCRLF;
                    } else {
                        return Err(McqError::ReqInvalid.error());
                    }
                }
                ReqPacketState::SpacesBeforeCas => {
                    if self.current() != b' ' {
                        if !self.current().is_ascii_digit() {
                            return Err(McqError::ReqInvalid.error());
                        }
                        self.unique_id = self.oft;
                        self.token = self.oft;
                        state = ReqPacketState::Cas;
                    }
                }
                ReqPacketState::Cas => {
                    if self.current().is_ascii_digit() {
                        break;
                    } else if self.current() == b' ' || self.current() == CR {
                        self.unique_id_len = self.oft - self.unique_id;
                        self.skip_back(1)?;
                        self.token = 0;
                        state = ReqPacketState::RunToCRLF;
                    } else {
                        return Err(McqError::ReqInvalid.error());
                    }
                }
                ReqPacketState::RunToVal => {
                    if self.current() == LF {
                        self.val_start = self.oft + 1;
                        state = ReqPacketState::Val;
                    } else {
                        return Err(McqError::ReqInvalid.error());
                    }
                }
                ReqPacketState::Val => {
                    m = self.oft + self.vlen;
                    if m >= self.data.len() {
                        return Err(super::Error::ProtocolIncomplete);
                    }
                    if self.data.at(m) == CR {
                        self.skip(self.vlen)?;
                        state = ReqPacketState::AlmostDone;
                    } else {
                        return Err(McqError::ReqInvalid.error());
                    }
                }
                ReqPacketState::SpacesBeforeNum => {
                    if self.current() != b' ' {
                        if !self.current().is_ascii_digit() {
                            return Err(McqError::ReqInvalid.error());
                        }
                        self.token = 0;
                        state = ReqPacketState::Num;
                    }
                }
                ReqPacketState::Num => {
                    if self.current().is_ascii_digit() {
                        // do nothing, just loop
                    } else if self.current() == b' ' || self.current() == CR {
                        self.token = 0;
                        self.skip_back(1)?;
                        state = ReqPacketState::RunToCRLF;
                    } else {
                        return Err(McqError::ReqInvalid.error());
                    }
                }
                ReqPacketState::RunToCRLF => {
                    let req_type = self.cmd_cfg().req_type();
                    match self.current() {
                        b' ' => break,
                        b'n' => {
                            if req_type.is_storage() || req_type.is_delete() {
                                self.token = 0;
                                state = ReqPacketState::Noreply;
                            } else {
                                return Err(McqError::ReqInvalid.error());
                            }
                        }
                        CR => {
                            if req_type.is_storage() {
                                state = ReqPacketState::RunToVal;
                            } else {
                                state = ReqPacketState::AlmostDone;
                            }
                        }
                        _ => {
                            return Err(McqError::ReqInvalid.error());
                        }
                    }
                }
                ReqPacketState::Noreply => {
                    if self.current() == b' ' || self.current() == CR {
                        m = self.token;
                        let nrlen = self.oft - m;
                        if nrlen == 7
                            && self
                                .data
                                .sub_slice(m, nrlen)
                                .start_with(m, &"noreply".as_bytes())?
                        {
                            let req_type = self.cmd_cfg().req_type();
                            assert!(req_type.is_storage() || req_type.is_delete());

                            self.token = 0;
                            self.noreply = true;
                            state = ReqPacketState::AfterNoreply;
                            self.skip_back(1)?;
                        } else {
                            return Err(McqError::ReqInvalid.error());
                        }
                    }
                }
                ReqPacketState::AfterNoreply => match self.current() {
                    b' ' => break,
                    CR => {
                        let req_type = self.cmd_cfg().req_type();
                        if req_type.is_storage() {
                            state = ReqPacketState::RunToVal;
                        } else {
                            state = ReqPacketState::AlmostDone;
                        }
                    }
                    _ => return Err(McqError::ReqInvalid.error()),
                },
                ReqPacketState::CRLF => match self.current() {
                    b' ' => break,
                    CR => state = ReqPacketState::AlmostDone,
                    _ => return Err(McqError::ReqInvalid.error()),
                },
                ReqPacketState::AlmostDone => {
                    if self.current() == LF {
                        return Ok(());
                    }
                    return Err(McqError::ReqInvalid.error());
                }
            }
        }
        Err(super::Error::ProtocolIncomplete)
    }

    #[inline]
    pub(super) fn take(&mut self) -> ds::MemGuard {
        assert!(self.oft_last < self.oft);
        let data = self.data.sub_slice(self.oft_last, self.oft - self.oft_last);
        self.oft_last = self.oft;

        self.stream.take(data.len())
    }
}

pub trait Packet {
    fn line(&self, oft: &mut usize) -> crate::Result<()>;
}

impl Packet for RingSlice {
    fn line(&self, oft: &mut usize) -> crate::Result<()> {
        if let Some(idx) = self.find_lf_cr(*oft) {
            *oft = idx +2;
            Ok(())
        } else {
            Err(crate::Error::ProtocolIncomplete)
        }
    }
}
// mcq 解析时状态
#[derive(Debug, Clone, Copy)]
pub(crate) enum ReqPacketState {
    Start = 1,
    ReqType,
    SpacesBeforeKey,
    Key,
    SpacesBeforeKeys,
    SpacesBeforeFlags,
    Flags,
    SpacesBeforeExpire,
    Expire,
    SpacesBeforeVlen,
    Vlen,
    SpacesBeforeCas,
    Cas,
    RunToVal,
    Val,
    SpacesBeforeNum,
    Num,
    RunToCRLF,
    CRLF,
    Noreply,
    AfterNoreply,
    AlmostDone,
}
