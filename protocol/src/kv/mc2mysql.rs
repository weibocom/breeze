use core::fmt::Write;
use std::fmt::Display;

use super::common::proto::codec::PacketCodec;
use crate::kv::MysqlBinary;
use crate::kv::{Binary, OP_ADD, OP_DEL, OP_GET, OP_GETK, OP_SET};
use crate::vector::flager::KvFlager;
use crate::vector::{
    Condition, GroupBy, Limit, Order, VectorCmd, COND_GROUP, COND_LIMIT, COND_ORDER,
};
use crate::{Error::FlushOnClose, Result};
use crate::{HashedCommand, Packet};
use ds::{RingSlice, Utf8};
use sharding::{distribution::DBRange, hash::Hasher};

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

    // TODO 先用最简单模式打通，稍后优化 fishermen
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

        Ok(vcmd)
    }

    #[inline]
    fn parse_vector_key(data: &Packet, key_pos: usize, vcmd: &mut VectorCmd) -> Result<()> {
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
            let idx = key_data.find(oft, ',' as u8).unwrap_or(key_data.len());
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
        // 解析fields，format: $4\r\nname\r\n$5\r\nvalue\r\n
        if field_start == 0 {
            return Ok(());
        }
        let mut oft = field_start;
        while oft < pos_after_field {
            let name = data.bulk_string(&mut oft)?;
            let value = data.bulk_string(&mut oft)?;
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
                if name.equal_ignore_case(COND_ORDER.as_bytes()) {
                    vcmd.order = Order {
                        field: op,
                        order: val,
                    };
                } else if name.equal_ignore_case(COND_LIMIT.as_bytes()) {
                    vcmd.limit = Limit {
                        offset: op,
                        limit: val,
                    };
                } else if name.equal_ignore_case(COND_GROUP.as_bytes()) {
                    vcmd.group_by = GroupBy { fields: val }
                } else {
                    vcmd.wheres.push(Condition::new(name, op, val));
                }
            }
            // 解析完fields，结束的位置应该刚好是data的末尾
            assert_eq!(oft, data.len(), "packet:{:?}", data);
        }
        Ok(())
    }
}
