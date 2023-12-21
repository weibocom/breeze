use core::fmt::Write;
use std::fmt::Display;

use super::common::proto::codec::PacketCodec;
use crate::kv::MysqlBinary;
use crate::kv::{Binary, OP_ADD, OP_DEL, OP_GET, OP_GETK, OP_SET};
use crate::vector;
use crate::vector::flager::KvFlager;
use crate::vector::{
    Condition, GroupBy, Limit, Order, VectorCmd, COND_GROUP, COND_LIMIT, COND_ORDER,
};
use crate::{Error::FlushOnClose, Result};
use crate::{HashedCommand, Packet};
use ds::{RingSlice, Utf8};
use sharding::{distribution::DBRange, hash::Hasher};

const FIELD_BYTES: &'static [u8] = b"FIELD";
const KVECTOR_SEPARATOR: u8 = b',';

pub trait Strategy {
    fn distribution(&self) -> &DBRange;
    fn hasher(&self) -> &Hasher;
    fn get_key(&self, _key: &RingSlice) -> u16;
    fn tablename_len(&self) -> usize;
    fn write_database_table(&self, buf: &mut impl Write, key: &RingSlice);
}

struct Table<'a, S> {
    strategy: &'a S,
    key: &'a RingSlice,
}

impl<'a, S: Strategy> Display for Table<'a, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.strategy.write_database_table(f, self.key);
        Ok(())
    }
}

impl<'a, S> Table<'a, S> {
    fn wrap_strategy(strategy: &'a S, key: &'a RingSlice) -> Self {
        Self { strategy, key }
    }
}

pub struct KeyVal<'a>(&'a RingSlice);
impl<'a> Display for KeyVal<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.visit(|c| escape_mysql_and_push(f, c));
        Ok(())
    }
}

struct SqlBuilder<'a, S> {
    op: u8,
    strategy: &'a S,
    key: &'a RingSlice,
    val: Option<RingSlice>,
}
impl<'a, S: Strategy> SqlBuilder<'a, S> {
    fn new(op: u8, strategy: &'a S, req: &HashedCommand, key: &'a RingSlice) -> Result<Self> {
        let val = match op {
            OP_ADD | OP_SET => Some(req.value()),
            OP_GET | OP_GETK | OP_DEL => None,
            _ => {
                return Err(FlushOnClose(
                    format!("not support op:{op}").into_bytes().into(),
                ))
            }
        };
        Ok(Self {
            op,
            strategy,
            key,
            val,
        })
    }
    fn len(&self) -> usize {
        let &Self {
            strategy,
            key,
            val,
            op,
        } = self;
        let (table_len, key_len) = (strategy.tablename_len(), key.len());
        match op {
            OP_ADD => {
                "insert into  (id,content) values (,'')".len()
                    + table_len
                    + key_len
                    + val.as_ref().unwrap().len()
                    + ESCAPED_GROW_LEN
            }
            OP_SET => {
                "update  set content='' where id=".len()
                    + table_len
                    + key_len
                    + val.as_ref().unwrap().len()
                    + ESCAPED_GROW_LEN
            }
            OP_DEL => "delete from  where id=".len() + table_len + key_len,
            OP_GET | OP_GETK => "select content from  where id=".len() + table_len + key_len,
            _ => panic!("not support op:{op}"),
        }
    }
    fn write_sql(&self, packet: &mut PacketCodec) {
        let &Self {
            strategy,
            key,
            val,
            op,
        } = self;
        let (table, key) = (Table::wrap_strategy(strategy, key), KeyVal(key));
        match op {
            OP_ADD => {
                let _ = write!(
                    packet,
                    "insert into {} (id,content) values ({},'{}')",
                    table,
                    key,
                    KeyVal(val.as_ref().unwrap())
                );
            }
            OP_SET => {
                let _ = write!(
                    packet,
                    "update {} set content='{}' where id={}",
                    table,
                    KeyVal(val.as_ref().unwrap()),
                    key
                );
            }
            OP_DEL => {
                let _ = write!(packet, "delete from {} where id={}", table, key);
            }
            OP_GET | OP_GETK => {
                let _ = write!(packet, "select content from {} where id={}", table, key);
            }
            _ => panic!("not support op:{op}"),
        };
    }
}

const ESCAPED_GROW_LEN: usize = 8;
pub struct MysqlBuilder;

// https://dev.mysql.com/doc/refman/8.0/en/string-literals.html
// Backslash (\) and the quote character used to quote the string must be escaped. In certain client environments, it may also be necessary to escape NUL or Control+Z.
// 应该只需要转义上面的，我们使用单引号，所以双引号也不转义了
pub fn escape_mysql_and_push(packet: &mut impl Write, c: u8) {
    //非法char要当成二进制push，否则会变成unicode
    let c = c as char;
    if c == '\\' || c == '\'' {
        let _ = packet.write_char('\\');
        let _ = packet.write_char(c);
    // } else if c == '\x00' {
    //     let _ = packet.write_char('\\');
    //     let _ = packet.write_char('0');
    // } else if c == '\n' {
    //     let _ = packet.write_char('\\');
    //     let _ = packet.write_char('n');
    // } else if c == '\r' {
    //     let _ = packet.write_char('\\');
    //     let _ = packet.write_char('r');
    // } else if c == '\x1a' {
    //     let _ = packet.write_char('\\');
    //     let _ = packet.write_char('Z');
    } else {
        let _ = packet.write_char(c);
    }
}

pub trait VectorSqlBuilder: MysqlBinary {
    fn len(&self) -> usize;
    fn write_sql(&self, buf: &mut impl Write);
}

impl MysqlBuilder {
    pub fn build_packets(
        strategy: &impl Strategy,
        req: &HashedCommand,
        key: &RingSlice,
    ) -> Result<Vec<u8>> {
        let op = req.op();
        let sql_builder = SqlBuilder::new(op, strategy, req, key)?;
        let sql_len = sql_builder.len();

        let mut packet = PacketCodec::default();
        packet.reserve(sql_len + 5);
        //5即下面两行包头长度
        packet.write_next_packet_header();
        packet.push(req.mysql_cmd() as u8);

        sql_builder.write_sql(&mut packet);

        packet.finish_current_packet();
        packet
            .check_total_payload_len()
            .map_err(|_| FlushOnClose(b"payload > max_allowed_packet"[..].into()))?;
        let packet: Vec<u8> = packet.into();
        log::debug!("build mysql packet:{}", packet.utf8());
        Ok(packet)
    }
    pub fn build_packets_for_vector(sql_builder: impl VectorSqlBuilder) -> Result<Vec<u8>> {
        let sql_len = sql_builder.len();

        let mut packet = PacketCodec::default();
        packet.reserve(sql_len + 5);
        //5即下面两行包头长度
        packet.write_next_packet_header();
        packet.push(sql_builder.mysql_cmd() as u8);

        sql_builder.write_sql(&mut packet);

        packet.finish_current_packet();
        packet
            .check_total_payload_len()
            .map_err(|_| FlushOnClose(b"payload > max_allowed_packet"[..].into()))?;
        let packet: Vec<u8> = packet.into();
        log::debug!("build mysql packet:{}", packet.utf8());
        Ok(packet)
    }

    /// 根据parse的结果，此处进一步获得kvector的detail/具体字段信息，以便进行sql构建
    pub fn parse_vector_detail(cmd: &HashedCommand) -> crate::Result<VectorCmd> {
        let data = Packet::from(cmd.sub_slice(0, cmd.len()));
        let flag = cmd.flag();

        let mut vcmd: VectorCmd = Default::default();
        // 解析keys
        Self::parse_vector_key(&data, flag.key_pos() as usize, &mut vcmd)?;

        // 解析fields
        let field_start = flag.field_pos() as usize;
        let condition_pos = flag.condition_pos() as usize;
        let pos_after_field = if condition_pos > 0 {
            const WHERE_LEN: usize = "$5\r\nWHERE\r\n".len();
            condition_pos - WHERE_LEN
        } else {
            data.len()
        };
        Self::parse_vector_field(&data, field_start, pos_after_field, &mut vcmd)?;

        // 解析conditions
        Self::parse_vector_condition(&data, condition_pos, &mut vcmd)?;

        // 校验cmd的合法性
        let cmd_type = vector::get_cmd_type(flag.op_code())?;
        Self::validate_cmd(&vcmd, cmd_type)?;

        Ok(vcmd)
    }

    #[inline]
    fn parse_vector_key(data: &Packet, key_pos: usize, vcmd: &mut VectorCmd) -> Result<()> {
        assert!(key_pos > 0, "{:?}", data);

        // 解析key，format: $-1\r\n or $2\r\nab\r\n
        let mut oft = key_pos;
        let key_data = data.bulk_string(&mut oft)?;
        // let mut keys = Vec::with_capacity(3);
        oft = 0;
        loop {
            if oft >= key_data.len() {
                break;
            }
            // 从keys中split出','分割的key
            let idx = key_data
                .find(oft, KVECTOR_SEPARATOR)
                .unwrap_or(key_data.len());
            vcmd.keys.push(key_data.sub_slice(oft, idx - oft));
            oft = idx + 1;
        }
        Ok(())
    }

    #[inline]
    fn parse_vector_field(
        data: &Packet,
        field_start: usize,
        pos_after_field: usize,
        vcmd: &mut VectorCmd,
    ) -> Result<()> {
        // field start pos为0，说明没有field
        if field_start == 0 {
            return Ok(());
        }
        let mut oft = field_start;
        while oft < pos_after_field {
            let name = data.bulk_string(&mut oft)?;
            let value = data.bulk_string(&mut oft)?;
            // 对于field关键字，value是field names
            if name.equal_ignore_case(FIELD_BYTES) {
                Self::validate_field_name(value)?;
            } else {
                // 否则 name就是field name
                Self::validate_field_name(name)?;
            };
            vcmd.fields.push((name, value));
        }
        // 解析完fields，结束的位置应该刚好是flag中记录的field之后下一个字节的oft
        assert_eq!(oft, pos_after_field, "packet:{:?}", data);

        Ok(())
    }

    fn parse_vector_condition(data: &Packet, cond_pos: usize, vcmd: &mut VectorCmd) -> Result<()> {
        // 解析where condition，必须是三段式format: $3\r\nsid\r\n$1\r\n<\r\n$3\r\n100\r\n
        vcmd.wheres = Vec::with_capacity(3);
        if cond_pos > 0 {
            let mut oft = cond_pos;
            while oft < data.len() {
                let name = data.bulk_string(&mut oft)?;
                let op = data.bulk_string(&mut oft)?;
                let val = data.bulk_string(&mut oft)?;
                if name.equal_ignore_case(COND_ORDER) {
                    // 先校验 order by 的value/fields
                    Self::validate_field_name(val)?;
                    vcmd.order = Order {
                        order: op,
                        field: val,
                    };
                } else if name.equal_ignore_case(COND_GROUP) {
                    // 先校验 group by 的value/fields
                    Self::validate_field_name(val)?;
                    vcmd.group_by = GroupBy { fields: val }
                } else if name.equal_ignore_case(COND_LIMIT) {
                    vcmd.limit = Limit {
                        offset: op,
                        limit: val,
                    };
                } else {
                    vcmd.wheres.push(Condition::new(name, op, val));
                }
            }
            // 解析完fields，结束的位置应该刚好是data的末尾
            assert_eq!(oft, data.len(), "packet:{:?}", data);
        }
        Ok(())
    }

    // /// TODO: 反引号方案，目前暂时不用，先注释掉 fishermen
    // /// 根据分隔符把field names分拆成多个独立的fields，并对field进行校验；
    // /// 校验策略：反引号方案，即ASCII: U+0001 .. U+007F，同时排除0、反引号;
    // /// https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
    // fn split_validate_field_name(field_names: RingSlice) -> Result<Vec<RingSlice>> {
    //     assert!(field_names.len() > 0, "field: {}", field_names);

    //     let len = field_names.len();
    //     let mut fields = Vec::with_capacity(len / 4 + 1);
    //     let mut start = 0;
    //     // 校验，并分拆fields
    //     for i in 0..len {
    //         let c = field_names.at(i);
    //         if MYSQL_FIELD_CHAR_TBL[c as usize] == 0 {
    //             return Err(crate::Error::RequestProtocolInvalid);
    //         }
    //         if c == KVECTOR_SEPARATOR {
    //             if i > start {
    //                 fields.push(field_names.sub_slice(start, i - start));
    //             }
    //             start = i + 1;
    //         }
    //     }
    //     if (len - 1) > start {
    //         fields.push(field_names.sub_slice(start, len - 1 - start));
    //     }

    //     Ok(fields)
    // }

    /// 校验fields，不可含有非法字符，避免sql注入
    fn validate_field_name(field_name: RingSlice) -> Result<()> {
        assert!(field_name.len() > 0, "field: {}", field_name);

        // 逐字节校验
        for i in 0..field_name.len() {
            let c = field_name.at(i);
            if MYSQL_FIELD_CHAR_TBL[c as usize] == 0 {
                return Err(crate::Error::RequestProtocolInvalid);
            }
        }

        Ok(())
    }

    /// 解析cmd detail完毕，开始根据cmd类型进行校验
    ///   1. vrange: 如果有fields，最多只能有1个，且name为“field”
    ///   2. vadd：fields必须大于0,不能where condition；
    ///   3. vupdate： fields必须大于0；
    ///   4. vdel： fields必须为空；
    ///   5. vcard：无；
    fn validate_cmd(vcmd: &VectorCmd, cmd_type: vector::CommandType) -> Result<()> {
        match cmd_type {
            vector::CommandType::VRange => {
                // vrange 的fields数量不能大于1
                if vcmd.fields.len() > 1
                    || (vcmd.fields.len() == 1 && !vcmd.fields[0].0.equal_ignore_case(FIELD_BYTES))
                {
                    return Err(crate::Error::RequestInvalidMagic);
                }
            }
            vector::CommandType::VAdd => {
                if vcmd.fields.len() == 0 || vcmd.wheres.len() > 0 {
                    return Err(crate::Error::RequestInvalidMagic);
                }
            }
            vector::CommandType::VUpdate => {
                if vcmd.fields.len() == 0 {
                    return Err(crate::Error::RequestInvalidMagic);
                }
            }
            vector::CommandType::VDel => {
                if vcmd.fields.len() > 0 {
                    return Err(crate::Error::RequestInvalidMagic);
                }
            }
            vector::CommandType::VCard => {}
            _ => {
                panic!("unknown kvector cmd:{:?}", vcmd);
            }
        }
        Ok(())
    }
}

/// mysql 非反引号方案 + 内建函数 + ‘,’，即field中只有如下字符在mesh中是合法的：
///  1. ASCII: [0-9,a-z,A-Z$_] (basic Latin letters, digits 0-9, dollar, underscore)
///  2. 内置函数符号17个：& >= < ( )等
///  3. 永久禁止：‘;’ 和空白符号；
///  具体见 #775
pub static MYSQL_FIELD_CHAR_TBL: [u8; 256] = [
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 1, 0, 0, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 0,
    0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 1, 1,
    0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
];

#[cfg(test)]
mod ValidateTest {
    use super::MYSQL_FIELD_CHAR_TBL;

    fn test_field_tbl() {
        // 校验nums
        let nums = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'];
        for c in nums {
            assert_eq!(MYSQL_FIELD_CHAR_TBL[c as usize], 1, "nums checked faield");
            println!("{} is ok", c);
        }

        // 校验大写字母
        const A: u8 = b'A';
        const a: u8 = b'a';
        for i in 0..26 {
            let upper = A + i;
            let lower = a + i;
            assert_eq!(
                MYSQL_FIELD_CHAR_TBL[upper as usize], 1,
                "upper alphabets checked faield"
            );
            assert_eq!(
                MYSQL_FIELD_CHAR_TBL[lower as usize], 1,
                "upper alphabets checked faield"
            );
            println!("{} and {} is ok", upper as char, lower as char);
        }

        // 校验特殊字符
        let special_chars = [
            33_u8, 37, 38, 40, 41, 42, 43, 44, 45, 46, 47, 58, 60, 61, 62, 94, 124,
        ];
        for c in special_chars {
            assert_eq!(
                MYSQL_FIELD_CHAR_TBL[c as usize], 1,
                "special checked faield"
            );
            println!("{} is ok", c);
        }

        // 必须禁止的字符
        // let forbidden_chars = [];
    }
}
