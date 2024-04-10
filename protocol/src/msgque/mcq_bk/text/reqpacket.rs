use ds::RingSlice;

use crate::Result;

use super::{
    command::{self, CommandProperties, RequestType},
    error::{self, McqError},
};
use std::time::{SystemTime, UNIX_EPOCH};

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
    cmd_cfg: Option<&'a CommandProperties>,
    flags: usize,
    flags_len: usize,
    flags_start: usize,
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
            cmd_cfg: None,
            flags: 0,
            flags_len: 0,
            flags_start: 0,
        }
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

    // 进入flags 解析阶段且current byte 满足协议要求，才更新flags 相关信息
    fn try_trace_flags(&mut self, state: ReqPacketState) {
        if state != ReqPacketState::Flags || !self.current().is_ascii_digit() {
            return;
        }
        if self.flags_start == 0 {
            self.flags_start = self.oft;
        }
        self.flags = self.flags * 10 + (self.current() - b'0') as usize;
        self.flags_len += 1;
    }

    // #[inline]
    // pub(super) fn state(&self) -> &ReqPacketState {
    //     &self.op_code
    // }

    #[inline]
    fn current(&self) -> u8 {
        assert!(self.available(), "oft:{}, rq:{:?}", self.oft, self.data);
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

    // 重置cmd cfg, 准备下一个req解析
    #[inline]
    pub fn prepare_for_parse(&mut self) {
        if self.available() {
            self.cmd_cfg = None;
        }
    }

    // TODO：简化 memcache 状态机，去掉对非mcq指令的字段解析
    pub(super) fn parse_req(&mut self) -> Result<()> {
        let mut m;
        let mut token = self.oft; // 记录某个token，like key、flag等
        let mut vlen = self.oft; // value len
        let mut state = ReqPacketState::Start;

        while self.available() {
            match state {
                ReqPacketState::Start => {
                    // mc 协议中cmd字符需要是小写
                    if !self.current().is_ascii_lowercase() {
                        return Err(McqError::ReqInvalid.into());
                    }
                    if self.current() != b' ' {
                        token = self.oft;
                        state = ReqPacketState::ReqType;
                    }
                }
                ReqPacketState::ReqType => {
                    // 直接加速skip掉cmd token
                    self.data.token(&mut self.oft, 0)?;
                    // 解析cmd
                    let cmd_name = self.data.sub_slice(token, self.oft - token);
                    let cfg = command::get_cfg_by_name(&cmd_name)?;
                    let req_type = cfg.req_type();
                    self.cmd_cfg = Some(cfg);

                    // 优化：对非单行指令过滤，只有set，需要拿到value len，目前其他指令都是单行 fishernmen
                    match req_type {
                        RequestType::Set => {
                            if self.current() == CR {
                                return Err(McqError::ReqInvalid.into());
                            }
                            state = ReqPacketState::SpacesBeforeKey;
                        }
                        RequestType::Get
                        | RequestType::Delete
                        | RequestType::Stats
                        | RequestType::Quit
                        | RequestType::Version => {
                            self.data.line(&mut self.oft)?;
                            return Ok(());
                        }
                        RequestType::Unknown => {
                            panic!("mcq unknown type msg:{:?}", self.data);
                        }
                    }
                }
                ReqPacketState::SpacesBeforeKey => {
                    if self.current() != b' ' {
                        token = self.oft;
                        state = ReqPacketState::Key;
                    }
                }
                ReqPacketState::Key => {
                    self.data.token(&mut self.oft, MAX_KEY_LEN)?;
                    state = ReqPacketState::SpacesBeforeFlags;
                    if self.current() == CR {
                        return Err(McqError::ReqInvalid.into());
                    }
                }
                ReqPacketState::SpacesBeforeFlags => {
                    if self.current() != b' ' {
                        if !self.current().is_ascii_digit() {
                            return Err(McqError::ReqInvalid.into());
                        }
                        self.skip_back(1)?;
                        state = ReqPacketState::Flags;
                    }
                }
                ReqPacketState::Flags => {
                    if self.current().is_ascii_digit() {
                        // 追踪flags段相关信息
                        self.try_trace_flags(state);
                    } else if self.current() == b' ' {
                        state = ReqPacketState::SpacesBeforeExpire;
                    } else {
                        return Err(McqError::ReqInvalid.into());
                    }
                }
                ReqPacketState::SpacesBeforeExpire => {
                    if self.current() != b' ' {
                        state = ReqPacketState::Expire;
                    }
                }
                ReqPacketState::Expire => {
                    if self.current().is_ascii_digit() {
                        // do nothing, just skip it
                    } else if self.current() == b' ' {
                        state = ReqPacketState::SpacesBeforeVlen;
                    } else {
                        return Err(McqError::ReqInvalid.into());
                    }
                }
                ReqPacketState::SpacesBeforeVlen => {
                    if self.current() != b' ' {
                        if !self.current().is_ascii_digit() {
                            return Err(McqError::ReqInvalid.into());
                        }
                        token = self.oft;
                        vlen = (self.current() - b'0') as usize;
                        state = ReqPacketState::Vlen;
                    }
                }
                ReqPacketState::Vlen => {
                    if self.current().is_ascii_digit() {
                        vlen = vlen * 10 + (self.current() - b'0') as usize;
                    } else if self.current() == CR {
                        state = ReqPacketState::RunToVal;
                    } else {
                        return Err(McqError::ReqInvalid.into());
                    }
                }
                ReqPacketState::RunToVal => {
                    if self.current() == LF {
                        state = ReqPacketState::Val;
                    } else {
                        return Err(McqError::ReqInvalid.into());
                    }
                }
                ReqPacketState::Val => {
                    m = self.oft + vlen;
                    if m >= self.data.len() {
                        return Err(super::Error::ProtocolIncomplete);
                    }
                    if self.data.at(m) == CR {
                        self.skip(vlen)?;
                        state = ReqPacketState::AlmostDone;
                    } else {
                        return Err(McqError::ReqInvalid.into());
                    }
                }
                ReqPacketState::AlmostDone => {
                    if self.current() == LF {
                        // skip掉LF，进入新的req位置
                        self.skip(1)?;
                        return Ok(());
                    }
                    return Err(McqError::ReqInvalid.into());
                }
            }

            // 当前字节处理完毕，继续下一个字节
            self.skip(1)?;
        }
        Err(super::Error::ProtocolIncomplete)
    }

    #[inline]
    pub(super) fn take(&mut self) -> ds::MemGuard {
        assert!(
            self.oft_last < self.oft,
            "oft: {}/{}, req:{:?}",
            self.oft_last,
            self.oft,
            self.data
        );
        let data = self.data.sub_slice(self.oft_last, self.oft - self.oft_last);
        self.oft_last = self.oft;

        self.stream.take(data.len())
    }

    // 将mcq req 文本协议（set key flags exp vlen）中 flags 段用timestamp 填充 ，消费时用作topic延时指标
    pub(super) fn mark_flags(
        &mut self,
        req_cmd: ds::MemGuard,
        req_type: RequestType,
    ) -> Result<ds::MemGuard> {
        // 只有set 指令 需要重置flags ，用作消费时间戳的填充
        if RequestType::Set != req_type {
            return Ok(req_cmd);
        }

        // TODO flags 如果业务已特殊设置，则先不予支持，然后根据实际场景确定方案？ fishermen
        if self.flags != 0 {
            log::warn!("flag in msgque should be 0 but: {}", self.flags);
            return Err(crate::Error::ProtocolNotSupported);
        }

        let time_sec = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .to_string();
        let mut req_data: Vec<u8> = Vec::with_capacity(req_cmd.len() + time_sec.len());

        //前: flags前半部分
        let pref_slice = req_cmd.sub_slice(0, self.flags_start - 0);
        pref_slice.copy_to_vec(&mut req_data);

        //中: 时间戳
        req_data.extend(time_sec.as_bytes());

        // 后：flags 后半部分
        let oft = self.flags_start + self.flags_len;
        let suffix_slice = req_cmd.sub_slice(oft, self.data.len() - oft);
        suffix_slice.copy_to_vec(&mut req_data);

        let marked_cmd = ds::MemGuard::from_vec(req_data);
        return Ok(marked_cmd);
    }
}

pub trait Packet {
    // 过滤掉一行
    fn line(&self, oft: &mut usize) -> crate::Result<()>;
    // 过滤一个token，直到空格或CR
    fn token(&self, oft: &mut usize, max_len: usize) -> crate::Result<()>;
}

impl Packet for RingSlice {
    #[inline]
    fn line(&self, oft: &mut usize) -> crate::Result<()> {
        if let Some(idx) = self.find_lf_cr(*oft) {
            *oft = idx + 2;
            Ok(())
        } else {
            Err(crate::Error::ProtocolIncomplete)
        }
    }

    // skip 一个token，必须非空格、CR开始，到空格、CR结束，长度必须大于0
    #[inline]
    fn token(&self, oft: &mut usize, max_len: usize) -> Result<()> {
        let end = if max_len == 0 {
            self.len()
        } else {
            max_len.min(self.len() - *oft) + *oft
        };

        for i in *oft..end {
            let c = self.at(i);
            if c == b' ' || c == CR {
                // token的长度不能为0
                let len = i - *oft;
                if len == 0 || (max_len > 0 && len >= max_len) {
                    return Err(error::McqError::ReqInvalid.into());
                }

                // 更新oft
                *oft = i;
                return Ok(());
            }
        }
        Err(super::Error::ProtocolIncomplete)
    }
}
// mcq 解析时状态
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum ReqPacketState {
    Start = 1,
    ReqType,
    SpacesBeforeKey,
    Key,
    // SpacesBeforeKeys,
    SpacesBeforeFlags,
    Flags,
    SpacesBeforeExpire,
    Expire,
    SpacesBeforeVlen,
    Vlen,
    RunToVal,
    Val,
    AlmostDone,
    // SpacesBeforeCas,
    // Cas,
    // SpacesBeforeNum,
    // Num,
    // RunToCRLF,
    // CRLF,
    // Noreply,
    // AfterNoreply,
}
