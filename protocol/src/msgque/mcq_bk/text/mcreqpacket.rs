//TODO 暂时保留mc备用
use ds::RingSlice;
use sharding::hash::{Bkdr, Hash};

use crate::{OpCode, Operation, Result, Utf8};

use super::error::McqError;

const CR: u8 = 13;
const LF: u8 = 10;
const MAX_KEY_LEN: usize = 250;

pub(super) struct RequestPacket<'a, S> {
    stream: &'a mut S,
    data: RingSlice,
    state: ReqPacketState,
    oft_last: usize,
    oft: usize,

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

    msg_type: MsgType,
}

impl<'a, S: crate::Stream> RequestPacket<'a, S> {
    #[inline]
    pub fn new(stream: &'a mut S) -> Self {
        let data = stream.slice();
        Self {
            stream,
            data,
            state: ReqPacketState::Start,
            oft_last: 0,
            oft: 0,
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
            msg_type: MsgType::Unknown,
        }
    }

    #[inline]
    pub(super) fn available(&self) -> bool {
        self.oft < self.data.len()
    }

    #[inline]
    pub(super) fn operation(&self) -> Operation {
        match self.msg_type {
            MsgType::Get => Operation::Get,
            MsgType::Set | MsgType::Delete => Operation::Store,
            MsgType::Quit | MsgType::Version | MsgType::Stats => Operation::Meta,
            _ => {
                log::warn!("+++ found unknow mcq msg: {:?}", self.data);
                Operation::Meta
            }
        }
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
    pub(super) fn state(&self) -> &ReqPacketState {
        &self.state
    }

    #[inline]
    fn current(&self) -> u8 {
        assert!(self.available(), "req:{:?}", self.data);
        self.data.at(self.oft)
    }

    // TODO：memcache 状态机，后续考虑优化状态机流程 fishermen
    pub(super) fn parse_req(&mut self) -> Result<()> {
        let mut state = self.state;
        let mut first_loop = true;
        let mut m;
        while self.available() {
            if first_loop {
                first_loop = false;
            } else {
                self.skip(1)?;
            }

            match state {
                ReqPacketState::Start => {
                    if !self.current().is_ascii_lowercase() {
                        return Err(McqError::ReqInvalid.error());
                    }
                    if self.current() != b' ' {
                        self.token = self.oft;
                        state = ReqPacketState::ReqType;
                    }
                }
                ReqPacketState::ReqType => {
                    if self.current() == b' ' || self.current() == CR {
                        let token = self.token;
                        self.token = 0;
                        self.msg_type = MsgType::Unknown;

                        let cmd_len = self.oft - token;
                        match cmd_len {
                            3 => {
                                if self.data.start_with(token, &"get ".as_bytes())? {
                                    self.msg_type = MsgType::Get;
                                } else if self.data.start_with(token, &"set ".as_bytes())? {
                                    self.msg_type = MsgType::Set;
                                }
                            }
                            4 => {
                                if self.data.start_with(token, &"quit".as_bytes())? {
                                    self.msg_type = MsgType::Quit;
                                }
                            }
                            5 => {
                                if self.data.start_with(token, &"stats".as_bytes())? {
                                    self.msg_type = MsgType::Stats;
                                }
                            }
                            6 => {
                                if self.data.start_with(token, &"delete".as_bytes())? {
                                    self.msg_type = MsgType::Delete;
                                }
                            }
                            7 => {
                                if self.data.start_with(token, &"version".as_bytes())? {
                                    self.msg_type = MsgType::Version;
                                }
                            }
                        }
                        match self.msg_type {
                            MsgType::Get | MsgType::Set | MsgType::Delete => {
                                if self.current() == CR {
                                    return Err(McqError::ReqInvalid.error());
                                }
                                state = ReqPacketState::SpacesBeforeKey;
                            }
                            MsgType::Quit => {
                                self.skip_back(1)?;
                                state = ReqPacketState::CRLF;
                            }
                            MsgType::Stats => {
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
                        self.token = self.oft;
                        self.key_start = self.oft;
                        state = ReqPacketState::Key;
                    }
                }
                ReqPacketState::Key => {
                    if self.current() == b' ' || self.current() == CR {
                        let len = self.oft - self.token;
                        if len > MAX_KEY_LEN {
                            return Err(McqError::ReqInvalid.error());
                        }
                        self.key_len = len;
                        self.token = 0;

                        if self.is_storage() {
                            state = ReqPacketState::SpacesBeforeFlags;
                        } else if self.is_delete() {
                            state = ReqPacketState::RunToCRLF;
                        } else if self.is_retrieval() {
                            state = ReqPacketState::SpacesBeforeKeys;
                        } else {
                            state = ReqPacketState::RunToCRLF;
                        }

                        if self.current() == CR {
                            if self.is_storage() {
                                return Err(McqError::ReqInvalid.error());
                            }
                            // 回退1byte，方便后面解析CRLF
                            self.skip_back(1)?;
                        }
                    }
                }
                ReqPacketState::SpacesBeforeKeys => {
                    match self.current() {
                        b' ' => {
                            // do nothing just skip it
                        }
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
                        self.token = self.oft;
                        self.flags = self.oft;
                        state = ReqPacketState::Flags;
                    }
                }
                ReqPacketState::Flags => {
                    if self.current().is_ascii_digit() {
                        // do nothing just skip it
                    } else if self.current() == b' ' {
                        self.flen = self.oft - self.flags;
                        self.token = 0;
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
                        self.token = self.oft;
                        state = ReqPacketState::Expire;
                    }
                }
                ReqPacketState::Expire => {
                    if self.current().is_ascii_digit() {
                        // do nothing just skip it
                    } else if self.current() == b' ' {
                        self.token = 0;
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
                        self.token = self.oft;
                        self.vlen = (self.current() - b'0') as usize;
                        state = ReqPacketState::Vlen;
                    }
                }
                ReqPacketState::Vlen => {
                    if self.current().is_ascii_digit() {
                        self.vlen = self.vlen * 10 + (self.current() - b'0') as usize;
                    } else if self.current() == b' ' || self.current() == CR {
                        self.real_vlen = self.vlen;
                        self.skip_back(1)?;
                        self.token = 0;
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
                        // do nothing just skip it
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
                        return Err(super::Error::ProtocolIncomplete(0));
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
                ReqPacketState::RunToCRLF => match self.current() {
                    b' ' => {
                        // do nothing, just skip it
                    }
                    b'n' => {
                        if self.is_storage() || self.is_delete() {
                            self.token = 0;
                            state = ReqPacketState::Noreply;
                        } else {
                            return Err(McqError::ReqInvalid.error());
                        }
                    }
                    CR => {
                        if self.is_storage() {
                            state = ReqPacketState::RunToVal;
                        } else {
                            state = ReqPacketState::AlmostDone;
                        }
                    }
                    _ => {
                        return Err(McqError::ReqInvalid.error());
                    }
                },
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
                            assert!(
                                self.is_storage() || self.is_delete(),
                                "req: {:?}",
                                self.data
                            );
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
                    b' ' => {
                        // do nothing, just skip
                    }
                    CR => {
                        if self.is_storage() {
                            state = ReqPacketState::RunToVal;
                        } else {
                            state = ReqPacketState::AlmostDone;
                        }
                    }
                    _ => return Err(McqError::ReqInvalid.error()),
                },
                ReqPacketState::CRLF => match self.current() {
                    b' ' => {
                        // do nothing just skip it
                    }
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
        Err(super::Error::ProtocolIncomplete(0))
    }

    #[inline]
    pub(super) fn take(&mut self) -> ds::MemGuard {
        assert!(self.oft_last < self.oft);
        let data = self.data.sub_slice(self.oft_last, self.oft - self.oft_last);
        self.oft_last = self.oft;

        self.stream.take(data.len())
    }

    // 后续可能会有更多指令支持
    #[inline]
    fn is_storage(&self) -> bool {
        match self.msg_type {
            MsgType::Set => true,
            _ => false,
        }
    }

    #[inline]
    fn is_delete(&self) -> bool {
        match self.msg_type {
            MsgType::Delete => true,
            _ => false,
        }
    }

    // 后续可能会有更多指令支持
    #[inline]
    fn is_retrieval(&self) -> bool {
        match self.msg_type {
            MsgType::Get => true,
            _ => false,
        }
    }

    pub fn noforward(&self) -> bool {
        match self.msg_type {
            MsgType::Quit | MsgType::Version => true,
            _ => false,
        }
    }
}

// mcq 解析时状态
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

pub(crate) struct CommandProperties {
    name: &'static str,
    op_code: OpCode,
    op: Operation,
    padding_rsp: u8,
    noforward: bool,
    supported: bool,
}

impl CommandProperties {
    #[inline]
    pub fn operation(&self) -> &Operation {
        &self.op
    }
    #[inline]
    pub fn op_code(&self) -> OpCode {
        self.op_code
    }
    #[inline]
    pub fn noforward(&self) -> bool {
        self.noforward
    }
}

// mcq2/mcq3有若干不同的指令
pub(super) struct Commands {
    supported: [CommandProperties; Self::MAPPING_RANGE],
    hash: Bkdr,
}

impl Commands {
    const MAPPING_RANGE: usize = 512;
    #[inline]
    fn new() -> Self {
        Self {
            supported: [CommandProperties::default(); Self::MAPPING_RANGE],
            hash: Bkdr::default(),
        }
    }
    #[inline]
    pub(crate) fn get_op_code(&self, name: &ds::RingSlice) -> u16 {
        let uppercase = UppercaseHashKey::new(name);
        let idx = self.hash.hash(&uppercase) as usize & (Self::MAPPING_RANGE - 1);
        // op_code 0表示未定义。不存在
        assert_ne!(idx, 0);
        idx as u16
    }
    #[inline]
    pub(crate) fn get_by_op(&self, op_code: u16) -> crate::Result<&CommandProperties> {
        assert!((op_code as usize) < self.supported.len());
        let cmd = unsafe { self.supported.get_unchecked(op_code as usize) };
        if cmd.supported {
            Ok(cmd)
        } else {
            Err(crate::Error::CommandNotSupported)
        }
    }
    // 不支持会返回协议错误
    #[inline]
    pub(crate) fn get_by_name(&self, cmd: &ds::RingSlice) -> crate::Result<&CommandProperties> {
        let uppercase = UppercaseHashKey::new(cmd);
        let idx = self.hash.hash(&uppercase) as usize & (Self::MAPPING_RANGE - 1);
        self.get_by_op(idx as u16)
    }

    fn add_support(&mut self, name: &'static str, op: Operation, padding_rsp: u8, noforward: bool) {
        let uppercase = name.to_uppercase();
        let idx = self.hash.hash(&uppercase.as_bytes()) as usize & (Self::MAPPING_RANGE - 1);
        assert!(idx < self.supported.len());
        // cmd 不能重复初始化
        assert!(!self.supported[idx].supported);

        self.supported[idx] = CommandProperties {
            name,
            op_code: idx as u16,
            op,
            padding_rsp,
            noforward,
            supported,
        };
    }
}

#[inline]
pub(super) fn get_op_code(cmd: &ds::RingSlice) -> u16 {
    SUPPORTED.get_op_code(cmd)
}
#[inline]
pub(super) fn get_cfg<'a>(op_code: u16) -> crate::Result<&'a CommandProperties> {
    SUPPORTED.get_by_op(op_code)
}

pub const PADDING_RSP_TABLE: [&str; 4] = [
    "",
    "SERVER_ERROR mcq not available\r\n",
    "VERSION 0.0.1\r\n",
    "STAT supported later\r\nEND\r\n",
];

lazy_static! {
    pub(super) static ref SUPPORTED: Commands = {
        let mut cmds = Commands::new();
        use Operation::*;
        for (name, op, padding_rsp, noforward) in vec![
            ("get", Get, 1, false),
            ("set", Store, 1, false),
            ("delete", Store, 1, false),

            ("version," Meta, 2, true),
            ("stats",   Meta, 3, true),
            ("quit",    Meta, 0, true),
        ] {
            cmds.add_support(name, op, padding_rsp, noforward);
        }
        cmds
    };
}

// mcq2 目前只支持如下指令
pub(crate) enum RequestType {
    Unknown,
    Get,
    Set,
    Delete,
    Version,
    Stats,
    Quit,
}
