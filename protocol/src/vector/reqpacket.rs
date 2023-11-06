use std::fmt::{self, Debug, Display, Formatter};

use ds::RingSlice;

use crate::{
    redis::{error::RedisError, packet::CRLF_LEN},
    Error, Flag, Packet, Result,
};

use super::command::CommandType;

/// key 最大长度限制为200
const MAX_KEY_LEN: u8 = 200;
/// 请求消息不能超过16M
const MAX_REQUEST_LEN: u32 = 1 << super::flager::CONDITION_POS_BITS - 1;

pub(crate) struct RequestPacket<'a, S> {
    stream: &'a mut S,
    data: Packet,
    /// redis协议中bulk的数量，如*4\r\n...，bulk的数量为4
    bulks: u16,
    pub(super) cmd_type: CommandType,
    // pub(super) flag: Flag,
    /// 每个完整cmd的起点
    oft_last: usize,
    oft: usize,
}

impl<'a, S: crate::Stream> RequestPacket<'a, S> {
    #[inline]
    pub(crate) fn new(stream: &'a mut S) -> Self {
        let data = stream.slice();
        Self {
            stream,
            data: data.into(),
            bulks: 0,
            cmd_type: Default::default(),
            // flag: Flag::new(),
            oft_last: 0,
            oft: 0,
        }
    }

    #[inline]
    pub(crate) fn available(&self) -> bool {
        self.oft < self.data.len()
    }

    #[inline]
    pub(crate) fn parse_bulk_num(&mut self) -> Result<()> {
        if self.bulks == 0 {
            debug_assert!(self.available(), "{:?}", self);
            if self.data[self.oft] != b'*' {
                return Err(RedisError::ReqInvalidStar.into());
            }
            self.bulks = self.data.num_of_bulks(&mut self.oft)? as u16;
            // 合法的kvector协议只会有2、3、4个对象
            assert!(2 <= self.bulks && self.bulks <= 4, "packet:{}", self);
        }
        Ok(())
    }

    // 解析完毕，如果数据未读完，需要保留足够的buff空间
    #[inline(always)]
    pub(crate) fn reserve_stream_buff(&mut self) {
        if self.oft > self.data.len() {
            log::debug!("+++ will reserve len:{}", (self.oft - self.data.len()));
            self.stream.reserve(self.oft - self.data.len())
        }
    }

    #[inline]
    pub(crate) fn inner_data(&self) -> &RingSlice {
        &self.data
    }

    #[inline]
    pub(crate) fn cmd_current_pos(&self) -> usize {
        self.oft - self.oft_last
    }

    #[inline]
    pub(crate) fn parse_cmd(&mut self) -> Result<Flag> {
        // cmd type如果为Unknow，说明尚未解析过
        assert_eq!(self.cmd_type, CommandType::Unknown);

        // 第一个bulk是bulk-string类型的cmd
        let cmd_len = self.data.num_of_string(&mut self.oft)?;
        let cmd = self.data.sub_slice(self.oft, cmd_len);
        let cmd_type: CommandType = cmd.into();
        self.oft += cmd_len + CRLF_LEN;

        // 初始化flag，接下来的解析，如果成功则持续设置flag
        use super::flager::KvFlager;
        let operation = cmd_type.operation();
        let mut flag = Flag::from_op(cmd_type as u16, operation);

        // 第二个bulk是bulk-string类型的key
        let key_pos = self.cmd_current_pos();
        if key_pos > (MAX_KEY_LEN as usize) {
            log::warn!("+++ kvector cmd too long: {}", self);
            return Err(Error::RequestProtocolInvalid);
        }
        flag.set_key_pos(key_pos as u8);
        let key_len = self.data.num_of_string(&mut self.oft)?;
        self.oft += key_len + CRLF_LEN;

        // 如果有第三个bulk，则是array 类型的$field_name $field_value pair
        if self.bulks > 2 {
            // 设置field pos
            let field_pos = self.cmd_current_pos();
            if field_pos >= (u8::MAX as usize) {
                log::warn!("+++ kvector key too long: {}", self);
                return Err(Error::RequestProtocolInvalid);
            }

            flag.set_field_pos(field_pos as u8);

            // check field count数量，field count必须是2的倍数，由name和value组成field pair
            let field_count = self.data.num_of_bulks(&mut self.oft)?;
            if field_count % 2 == 1 || field_count >= u8::MAX as usize {
                log::warn!("+++ kvector req fields count malformed: {}", self);
                return Err(Error::RequestProtocolInvalid);
            }

            // skip 掉所有的field tokens
            self.data.skip_bulks(&mut self.oft, field_count as usize)?;
        }

        // 如果有四个bulk，则第四个是array类型 的where condition
        if self.bulks > 3 {
            // 设置condition pos
            let condition_pos = self.cmd_current_pos();
            if condition_pos >= MAX_REQUEST_LEN as usize {
                log::warn!("+++ too big request:{}", self);
                return Err(Error::RequestProtocolInvalid);
            }
            flag.set_condition_pos(condition_pos as u32);

            // check condition bulk数量
            let condition_count = self.data.num_of_bulks(&mut self.oft)?;
            if (condition_count - 1) % 3 != 0 {
                log::warn!("+++ kvector req condition count malformed: {}", self);
                return Err(Error::RequestProtocolInvalid);
            }

            // skip 掉where condition bulks
            self.data.skip_bulks(&mut self.oft, condition_count)?;
        }

        Ok(flag)
    }

    #[inline]
    pub(super) fn take(&mut self) -> ds::MemGuard {
        assert!(self.oft_last < self.oft, "packet:{}", self);
        let data = self.data.sub_slice(self.oft_last, self.oft - self.oft_last);
        self.oft_last = self.oft;
        // 重置bulks和cmd_type
        self.bulks = 0;
        self.cmd_type = CommandType::Unknown;
        self.stream.take(data.len())
    }
}

impl<'a, S: crate::Stream> Display for RequestPacket<'a, S> {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "(packet => len:{} bulk num: {} cmd_type:{:?} oft:({} => {})) data:{:?}",
            self.data.len(),
            self.bulks,
            self.cmd_type,
            self.oft_last,
            self.oft,
            self.data
        )
    }
}
impl<'a, S: crate::Stream> Debug for RequestPacket<'a, S> {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}
