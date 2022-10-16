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

    // #[inline]
    // fn skip_back(&mut self, count: usize) -> Result<()> {
    //     self.oft -= count;
    //     if self.oft >= self.oft_last {
    //         return Ok(());
    //     }
    //     Err(McqError::ReqInvalid.error())
    // }

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
                    if self.current() == b' ' {
                        break;
                    }
                    if !self.current().is_ascii_lowercase() {
                        return Err(McqError::ReqInvalid.error());
                    }
                    token = self.oft;
                    state = ReqPacketState::ReqType;
                    continue;
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
                                return Err(McqError::ReqInvalid.error());
                            }
                            state = ReqPacketState::SpacesBeforeKey;
                            continue;
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
                            panic!("mcq unknown type should not come here");
                        }
                    }
                }
                ReqPacketState::SpacesBeforeKey => {
                    if self.current() != b' ' {
                        token = self.oft;
                        state = ReqPacketState::Key;
                        continue;
                    }
                }
                ReqPacketState::Key => {
                    self.data.token(&mut self.oft, MAX_KEY_LEN)?;
                    state = ReqPacketState::SpacesBeforeFlags;
                    if self.current() == CR {
                        return Err(McqError::ReqInvalid.error());
                    }
                    continue;
                }
                ReqPacketState::SpacesBeforeFlags => {
                    if self.current() != b' ' {
                        if !self.current().is_ascii_digit() {
                            return Err(McqError::ReqInvalid.error());
                        }
                        state = ReqPacketState::Flags;
                        continue;
                    }
                }
                ReqPacketState::Flags => {
                    if self.current().is_ascii_digit() {
                        //break;
                    } else if self.current() == b' ' {
                        state = ReqPacketState::SpacesBeforeExpire;
                        continue;
                    } else {
                        return Err(McqError::ReqInvalid.error());
                    }
                }
                ReqPacketState::SpacesBeforeExpire => {
                    if self.current() != b' ' {
                        state = ReqPacketState::Expire;
                        continue;
                    }
                }
                ReqPacketState::Expire => {
                    if self.current().is_ascii_digit() {
                        // break;
                    } else if self.current() == b' ' {
                        state = ReqPacketState::SpacesBeforeVlen;
                        continue;
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
                        vlen = 0;
                        state = ReqPacketState::Vlen;
                        continue;
                    }
                }
                ReqPacketState::Vlen => {
                    if self.current().is_ascii_digit() {
                        vlen = vlen * 10 + (self.current() - b'0') as usize;
                    } else if self.current() == CR {
                        state = ReqPacketState::RunToVal;
                    } else {
                        return Err(McqError::ReqInvalid.error());
                    }
                }
                ReqPacketState::RunToVal => {
                    if self.current() == LF {
                        state = ReqPacketState::Val;
                    } else {
                        return Err(McqError::ReqInvalid.error());
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
                        return Err(McqError::ReqInvalid.error());
                    }
                }
                ReqPacketState::AlmostDone => {
                    if self.current() == LF {
                        // skip掉LF，进入新的req位置
                        self.skip(1)?;
                        return Ok(());
                    }
                    return Err(McqError::ReqInvalid.error());
                }
            }

            // 当前字节处理完毕，继续下一个字节
            self.skip(1)?;
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

    // 将mcq req 文本协议（set key flags exp vlen）中 flags 段用timestamp 填充 ，消费时用作topic延时指标
    pub(super) fn mark_flags(
        &mut self,
        req_cmd: ds::MemGuard,
        req_type: RequestType,
    ) -> ds::MemGuard {
        if RequestType::Get == req_type {
            return req_cmd;
        }
        let mut req_data: Vec<u8> = Vec::with_capacity(req_cmd.data().len() + 10);
        req_cmd.data().copy_to_vec(&mut req_data);
        if let Ok((key_start, _op_len)) = next_token(&req_data, 0) {
            if let Ok((flags_start, _ken_len)) = next_token(&req_data, key_start) {
                if let Ok((_expire_start, flags_len)) = next_token(&req_data, flags_start) {
                    // 业务特殊需要，则不做标记 直接返回
                    if flags_len > 1 || req_data[flags_start] != b'0' {
                        return req_cmd;
                    }
                    let time_string = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs()
                        .to_string();
                    // 为避免重新开辟一块vec空间，先通过空格填充占位，再通过移动覆盖
                    for _i in 0..time_string.len() - 1 {
                        req_data.push(b' ');
                    }
                    let mut i = req_data.len() - 1;
                    let mut j = i - (time_string.len() - 1);
                    while j > flags_start {
                        req_data[i] = req_data[j];
                        j -= 1;
                        i -= 1;
                    }
                    for (idx, num) in time_string.as_bytes().iter().enumerate() {
                        req_data[flags_start + idx] = num.to_owned();
                    }
                    let marked_cmd = ds::MemGuard::from_vec(req_data);
                    return marked_cmd;
                }
            }
        }
        return req_cmd;
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
                    return Err(error::McqError::ReqInvalid.error());
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
#[derive(Debug, Clone, Copy)]
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

// eg: oft = 0   set xx O 0 123\r\n  返回 (0,3,1) ,表示文本分段开始,token 长度
pub(super) fn next_token(data_body: &Vec<u8>, cur_start: usize) -> Result<(usize, usize)> {
    let end = data_body.len();
    let mut words_len: usize = 0;
    let mut space_len: usize = 0;
    if cur_start >= data_body.len() {
        return Err(error::McqError::ReqInvalid.error());
    }

    // word 计数
    for i in cur_start..end {
        if data_body[i] != b' ' {
            words_len += 1;
            continue;
        }
        break;
    }

    // 空格数
    for i in (cur_start + words_len)..end {
        if data_body[i] == b' ' {
            space_len += 1;
            continue;
        }
        break;
    }
    Ok((cur_start + space_len + words_len, words_len))
}
