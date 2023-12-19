use super::{
    command::CommandProperties, flager::KvFlager, OP_VADD, OP_VCARD, OP_VDEL, OP_VRANGE, OP_VUPDATE,
};
use ds::RingSlice;

use std::fmt::{self, Debug, Display, Formatter};

use crate::{
    redis::{command::CommandHasher, packet::CRLF_LEN},
    vector::{command, error::KvectorError},
    Flag, Packet, Result,
};

/// key 最大长度限制为200
const MAX_KEY_LEN: usize = 200;
/// 请求消息不能超过16M
const MAX_REQUEST_LEN: usize = (1 << super::flager::CONDITION_POS_BITS) - 1;
const BYTES_WHERE: &'static [u8] = b"WHERE";
const BYTES_FIELD: &'static [u8] = b"FIELD";

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

        // 如果request不带fields、condition则，此时bulks应该为0
        if self.bulks == 0 {
            return Ok(flag);
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

    /// 前置条件： bulks大于0
    fn parse_fields(&mut self, flag: &mut Flag) -> Result<()> {
        assert!(self.bulks > 0, "{:?}", self);

        // 解析并校验field
        match flag.op_code() {
            OP_VRANGE => return self.parse_vrange_fields(flag),
            OP_VUPDATE | OP_VADD => return self.parse_vadd_vupdate_fields(flag),
            OP_VDEL | OP_VCARD => {
                // vdel、vcard没有condition，只可能有where conditions
                let token = self.next_bulk_string()?;
                if token.equal_ignore_case(BYTES_WHERE) && self.bulks > 0 {
                    return Ok(());
                }
                return Err(KvectorError::ReqNotSupported.into());
            }
            _ => {
                panic!("+++ should not come here:{:?}", self);
            }
        }
    }

    fn parse_vrange_fields(&mut self, flag: &mut Flag) -> Result<()> {
        // key 后面的字段可能是field，也可能是where conditioncheck
        let pos = self.cmd_current_pos();
        let mut token = self.next_bulk_string()?;

        // vrange对应的格式field $field_names，其中field_names以逗号分隔
        if token.equal_ignore_case(BYTES_FIELD) {
            // 设置field的pos
            flag.set_field_pos(pos as u8);

            // 校验field names
            let field_names = self.next_bulk_string()?;
            if !validate_field_name(field_names) {
                log::warn!("+++ malformed req:{:?}", self);
                return Err(KvectorError::ReqNotSupported.into());
            }

            // bulkd为0，说明没有where condition
            if self.bulks == 0 {
                return Ok(());
            }
            token = self.next_bulk_string()?;
        }

        // 解析完field，当前的token必须是where，且where之后必须还有conditions
        if token.equal_ignore_case(BYTES_WHERE) && self.bulks > 0 {
            return Ok(());
        }

        log::warn!("+++ malformed vdel/vcard req:{:?}", self);
        return Err(KvectorError::ReqNotSupported.into());
    }

    fn parse_vadd_vupdate_fields(&mut self, flag: &mut Flag) -> Result<()> {
        // key 后面的字段可能是field，也可能是where conditioncheck
        let pos = self.cmd_current_pos();
        let mut token = self.next_bulk_string()?;

        // 格式：$field_name $field_value
        if !token.equal_ignore_case(BYTES_WHERE) {
            // 第0个token，不是where，那就一定是field了，设置field的pos
            flag.set_field_pos(pos as u8);

            // validate field name
            if !validate_field_name(token) {
                return Err(KvectorError::ReqNotSupported.into());
            }

            // 下一个token是field_value，不是field name
            let mut is_next_field_name = false;
            // 接下来loop skip 掉所有的剩余的所有$field_name $field_value pair
            loop {
                if !is_next_field_name {
                    if self.bulks == 0 {
                        return Err(KvectorError::ReqNotSupported.into());
                    }
                    // field value直接skip
                    self.skip_bulks(1)?;
                } else {
                    if self.bulks == 0 {
                        return Ok(());
                    }
                    // 可能是field，也可能是where，需要读出来进行check
                    let next = self.next_bulk_string()?;
                    // 遇到 where，break出loop
                    if next.equal_ignore_case(BYTES_WHERE) {
                        token = next;
                        break;
                    }
                    // 不是where，肯定是field name，需要进行校验，校验失败则返回异常
                    if !validate_field_name(next) {
                        return Err(KvectorError::ReqNotSupported.into());
                    }
                }

                is_next_field_name = !is_next_field_name;
            }
        }

        // 至此，token必须是where，且剩余bulks必须大于0
        if token.equal_ignore_case(BYTES_WHERE) && self.bulks > 0 {
            return Ok(());
        }

        log::warn!("+++ malformed vdel/vcard req:{:?}", self);
        return Err(KvectorError::ReqNotSupported.into());
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

/// 校验mysql的field name，根据反引号的ASCII规则加强版，即ASCII: U+0001 .. U+007F，同时排除0、反引号;
/// 附加逻辑：结尾不可为','，其为fields分隔符，每个','后跟一个field
/// https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
fn validate_field_name(field_name: RingSlice) -> bool {
    assert!(field_name.len() > 0, "field: {}", field_name);

    let len = field_name.len();
    for i in 0..len {
        let c = field_name.at(i);
        if MYSQL_FIELD_CHAR_TBL[c as usize] == 0 {
            return false;
        }
    }

    // 至此，只要最后一个字节不为逗号即合法
    field_name.at(len - 1) != b','
}

/// mysql 反引号方案，即目前只支持：ASCII: U+0001 .. U+007F，同时排除0、反单引号(96)。
pub static MYSQL_FIELD_CHAR_TBL: [u8; 256] = [
    0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
];

// /// mysql 非引号方案：ASCII: [0-9,a-z,A-Z$_] (basic Latin letters, digits 0-9, dollar, underscore)，如果合法，对应位置的值为1
// pub static MYSQL_FIELD_CHAR_TBL: [u8; 256] = [
//     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
//     0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0,
//     0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 1,
//     0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0,
//     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
//     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
//     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
//     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
// ];

// /// mysql field 带的多个字段名的字符checking table，不同字段名之间用','分隔，用于check多field_name的字符合法性，如果合法，对应位置的值为1
// /// 相对于单个field name，多了一个逗号/",", 逗号的ascii码值为44
// pub static MYSQL_FIELD_AND_COMMA_CHAR_TBL: [u8; 256] = [
//     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
//     0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0,
//     0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 1,
//     0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0,
//     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
//     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
//     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
//     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
// ];
