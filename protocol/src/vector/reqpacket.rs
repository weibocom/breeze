use super::flager::KvFlager;
use ds::RingSlice;

use std::fmt::{self, Debug, Display, Formatter};

use crate::{redis::error::RedisError, Error, Flag, Packet, Result};

use super::command::CommandType;

/// key 最大长度限制为200
const MAX_KEY_LEN: usize = 200;
/// 请求消息不能超过16M
const MAX_REQUEST_LEN: usize = 1 << super::flager::CONDITION_POS_BITS - 1;
const WHERE: &str = "WHERE";

pub(crate) struct RequestPacket<'a, S> {
    stream: &'a mut S,
    // TODO 对data的读写，除了bulks 数量，其他都必须走本地的代理方法，避免本地变量未更新
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
            // 合法的kvector协议至少有2个bulk string
            assert!(self.bulks >= 2, "packet:{}", self);
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
        // 第一个字段是cmd type，根据cmd type构建flag，并在后续解析中设置flag
        self.parse_cmd_name()?;
        let mut flag = Flag::from_op(self.cmd_type.clone() as u16, self.cmd_type.operation());

        // 第二个字段是key
        self.parse_key(&mut flag)?;

        // 解析fields，因为要对比where，所以如果有where，也会消费掉
        self.parse_fields(&mut flag)?;

        // 解析condition
        self.parse_condition(&mut flag)?;

        Ok(flag)
    }

    #[inline]
    fn parse_cmd_name(&mut self) -> Result<()> {
        // 第一个bulk是bulk-string类型的cmd
        let cmd = self.next_bulk_string()?;
        self.cmd_type = cmd.into();
        assert_eq!(self.cmd_type, CommandType::Unknown);
        Ok(())
    }

    /// 读取下一个bulk string，bulks会减1
    #[inline]
    fn next_bulk_string(&mut self) -> Result<RingSlice> {
        if self.bulks == 0 {
            log::warn!("no more bulk string req:{}", self);
            return Err(Error::RequestProtocolInvalid);
        }
        let next = self.data.bulk_string(&mut self.oft)?;
        self.bulks -= 1;
        Ok(next)
    }

    /// skip 掉n个bulk
    #[inline]
    fn skip_bulks(&mut self, count: u16) -> Result<()> {
        if count > self.bulks {
            log::warn!("not enough bulks to skip req:{}", self);
            return Err(Error::RequestProtocolInvalid);
        }
        self.bulks -= count;
        self.data.skip_bulks(&mut self.oft, count as usize)
    }

    #[inline]
    fn parse_key(&mut self, flag: &mut Flag) -> Result<()> {
        // 第二个bulk是bulk-string类型的key
        let key_pos = self.cmd_current_pos();
        if key_pos > (MAX_KEY_LEN as usize) {
            log::warn!("+++ kvector cmd too long: {}", self);
            return Err(Error::RequestProtocolInvalid);
        }
        flag.set_key_pos(key_pos as u8);
        let key = self.next_bulk_string()?;
        if key.len() < MAX_KEY_LEN {
            return Ok(());
        }

        log::warn!("+++ kvector key too long: {}", self);
        return Err(Error::RequestProtocolInvalid);
    }

    fn parse_fields(&mut self, flag: &mut Flag) -> Result<()> {
        // key 后面的字段可能是field，也可能是where conditioncheck
        let pos = self.cmd_current_pos();
        let field_or_where = self.next_bulk_string()?;
        if !field_or_where.equal_ignore_case(WHERE.as_bytes()) {
            // 不是where，就是field了，设置field的pos
            flag.set_field_pos(pos as u8);

            // skip 掉所有的剩余的所有field tokens
            let mut field_idx = 1;
            loop {
                // field 是key value pair 对，idx从0开始计数，如 [k1 v1 k2 v2] 的idx [0, 1, 2, 3]
                // 没有bulks时，如果idx正好是奇数位，说明协议非法，少了value;如果idx为偶数，说明解析完毕；
                if self.bulks == 0 {
                    if field_idx & 2 == 1 {
                        log::warn!("+++ invalid field count:{}", self);
                        return Err(Error::RequestProtocolInvalid);
                    } else {
                        return Ok(());
                    }
                };

                // 还有bulks，检测奇偶位
                if field_idx & 2 == 1 {
                    // 奇数位的value直接skip
                    self.skip_bulks(1)?;
                } else {
                    // 偶数位，可能是where，需要读出来进行check
                    let next = self.next_bulk_string()?;
                    if next.equal_ignore_case(WHERE.as_bytes()) {
                        if self.bulks == 0 {
                            log::warn!("+++ empty where condition req:{}", self);
                            return Err(Error::RequestProtocolInvalid);
                        }
                        return Ok(());
                    }
                    // 当前不是where，则肯定是field，则其后面必须还要有val
                }
                field_idx += 1;
            }
        }
        Ok(())
    }

    fn parse_condition(&mut self, flag: &mut Flag) -> Result<()> {
        // 如果bulks为0，说明没有condition 了
        if self.bulks == 0 {
            return Ok(());
        }

        // 剩下肯定全部是condition，必须是3的倍数
        if self.bulks % 3 != 0 {
            log::warn!("+++ kvector req condition count malformed: {}", self);
            return Err(Error::RequestProtocolInvalid);
        }
        // 设置condition pos
        let condition_pos = self.cmd_current_pos();
        if condition_pos >= MAX_REQUEST_LEN as usize {
            log::warn!("+++ too big request:{}", self);
            return Err(Error::RequestProtocolInvalid);
        }
        flag.set_condition_pos(condition_pos as u32);
        // skip 掉condition 的 bulks
        self.skip_bulks(self.bulks)?;

        assert_eq!(self.bulks, 0, "kvector:{}", self);
        Ok(())
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
