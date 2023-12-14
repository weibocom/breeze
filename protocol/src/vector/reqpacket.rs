use super::{command::CommandProperties, flager::KvFlager};
use ds::RingSlice;

use std::fmt::{self, Debug, Display, Formatter};

use crate::{
    redis::{command::CommandHasher, packet::CRLF_LEN},
    vector::{command, error::KvectorError},
    Error, Flag, Packet, Result,
};

/// key 最大长度限制为200
const MAX_KEY_LEN: usize = 200;
/// 请求消息不能超过16M
const MAX_REQUEST_LEN: usize = (1 << super::flager::CONDITION_POS_BITS) - 1;
const BYTES_WHERE: &'static [u8] = b"WHERE";

pub(crate) struct RequestPacket<'a, S> {
    stream: &'a mut S,
    /// 对data的读写，除了bulks 数量，其他都必须走本地的代理方法，避免本地变量未更新
    data: Packet,
    /// redis协议中bulk的数量，如*4\r\n...，bulk的数量为4
    bulks: u16,
    pub op_code: u16,
    // pub(super) cmd_type: CommandType,
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
            op_code: Default::default(),
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
        assert_eq!(self.bulks, 0, "{:?}", self);
        assert!(self.available(), "{:?}", self);
        log::debug!("+++ parse bulk_num: {}:{:?}", self.oft, self);

        if self.data[self.oft] != b'*' {
            return Err(KvectorError::ReqInvalidStar.into());
        }
        self.bulks = self.data.num_of_bulks(&mut self.oft)? as u16;
        // 合法的kvector协议至少有2个bulk string，但redis sdk的ping/select 等cmd格式不同
        // assert!(self.bulks >= 2, "packet:{}", self);

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
        let cfg = self.parse_cmd_properties()?;
        let mut flag = cfg.flag();

        // meta cmd 当前直接返回固定响应，直接skip后面的tokens即可
        if cfg.op.is_meta() {
            self.skip_bulks(self.bulks)?;
            return Ok(flag);
        }

        // 非meta的cmd，有key，可能有fields和where condition，全部解析出位置，并记录到flag
        // 如果有key，则下一个字段是key
        if cfg.has_key {
            self.parse_key(&mut flag)?;
        }

        // 如果有field，则接下来解析fields，不管是否有field，统一先把where这个token消费掉，方便后续统一从condition位置解析
        if cfg.can_hold_field {
            self.parse_fields(&mut flag)?;
        } else if self.bulks > 0 && cfg.can_hold_where_condition {
            // 如果还有bulks，且该cmd可以hold where condition，此处肯定是where token，直接读出skip掉
            let token = self.next_bulk_string()?;
            if !token.equal_ignore_case(BYTES_WHERE) || self.bulks < 1 {
                // 没有where，或者有where，但剩余bulks为0则返回异常
                return Err(KvectorError::ReqNotSupported.into());
            }
        }
        log::debug!("++++ after field parsedoft:{}", self.oft);

        // 如果有condition，解析之，注意保证where token已经被skip掉了
        if cfg.can_hold_where_condition {
            self.parse_condition(&mut flag)?;
        }

        log::debug!("++++ after condition parsed oft:{}", self.oft);
        Ok(flag)
    }

    #[inline]
    pub(crate) fn parse_cmd_properties(&mut self) -> Result<&'static CommandProperties> {
        // 当前上下文是获取命令。格式为:  $num\r\ncmd\r\n
        if let Some(first_r) = self.data.find(self.oft, b'\r') {
            debug_assert_eq!(self.data[self.oft], b'$', "{:?}", self);
            // 路过CRLF_LEN个字节，通过命令获取op_code
            let (op_code, idx) = CommandHasher::hash_slice(&*self.data, first_r + CRLF_LEN)?;
            if idx + CRLF_LEN > self.data.len() {
                return Err(crate::Error::ProtocolIncomplete);
            }
            self.op_code = op_code;
            // 第一次解析cmd需要对协议进行合法性校验
            let cfg = command::get_cfg(op_code)?;
            cfg.validate(self.bulks as usize)?;

            // check 命令长度
            debug_assert_eq!(cfg.name.len(), self.data.str_num(self.oft + 1..first_r));

            // cmd name 解析完毕，bulk 减 1
            self.oft = idx + CRLF_LEN;
            self.bulks -= 1;
            Ok(cfg)
        } else {
            return Err(crate::Error::ProtocolIncomplete);
        }
    }

    // #[inline]
    // fn parse_cmd_name(&mut self) -> Result<()> {
    //     // 第一个bulk是bulk-string类型的cmd
    //     let cmd = self.next_bulk_string()?;
    //     self.cmd_type = cmd.into();
    //     if self.cmd_type.is_invalid() {
    //         return Err(Error::FlushOnClose(ERR_UNSUPPORT_CMD.into()));
    //     }
    //     Ok(())
    // }

    /// 读取下一个bulk string，bulks会减1
    #[inline]
    fn next_bulk_string(&mut self) -> Result<RingSlice> {
        if self.bulks == 0 {
            log::warn!("no more bulk string req:{}", self);
            return Err(KvectorError::ReqNotSupported.into());
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
            return Err(KvectorError::ReqInvalidBulkNum.into());
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
            return Err(KvectorError::ReqNotSupported.into());
        }
        flag.set_key_pos(key_pos as u8);
        let key = self.next_bulk_string()?;
        if key.len() < MAX_KEY_LEN {
            return Ok(());
        }

        log::warn!("+++ kvector key too long: {}", self);
        return Err(KvectorError::ReqNotSupported.into());
    }

    fn parse_fields(&mut self, flag: &mut Flag) -> Result<()> {
        // key 后面的字段可能是field，也可能是where conditioncheck
        let pos = self.cmd_current_pos();
        let token = self.next_bulk_string()?;
        if !token.equal_ignore_case(BYTES_WHERE) {
            // 第0个token，不是where，那就一定是field了，设置field的pos
            flag.set_field_pos(pos as u8);

            // skip 掉所有的剩余的所有field tokens
            let mut field_idx = 1;
            loop {
                // field 是key value pair 对，idx从0开始计数，如 [k1 v1 k2 v2] 的idx [0, 1, 2, 3]
                // 没有bulks时，如果idx正好是奇数位，说明协议非法，少了value;如果idx为偶数，说明解析完毕；
                if self.bulks == 0 {
                    if field_idx & 2 == 1 {
                        log::warn!("+++ invalid field count:{}", self);
                        return Err(KvectorError::ReqNotSupported.into());
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
                    if next.equal_ignore_case(BYTES_WHERE) {
                        if self.bulks == 0 {
                            log::warn!("+++ empty where condition req:{}", self);
                            return Err(KvectorError::ReqInvalidBulkNum.into());
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
            return Err(KvectorError::ReqInvalidBulkNum.into());
        }
        // 设置condition pos
        let condition_pos = self.cmd_current_pos();
        if condition_pos >= MAX_REQUEST_LEN as usize {
            log::warn!("+++ too big request:{}", self);
            return Err(KvectorError::ReqNotSupported.into());
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
        // self.cmd_type = CommandType::Unknown;
        self.op_code = 0;
        self.stream.take(data.len())
    }
}

impl<'a, S: crate::Stream> Display for RequestPacket<'a, S> {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "(packet => len:{} bulk num: {} oft:({} => {})) data:{:?}",
            self.data.len(),
            self.bulks,
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
