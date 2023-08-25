use core::fmt::Write;
use enum_dispatch::enum_dispatch;

use super::common::proto::codec::PacketCodec;
use crate::kv::MysqlBinary;
use crate::kv::{Binary, OP_ADD, OP_DEL, OP_GET, OP_GETK, OP_SET};
use crate::HashedCommand;
use crate::{Error::MysqlError, Result};
use ds::RingSlice;
use sharding::{distribution::DBRange, hash::Hasher};

#[enum_dispatch]
pub trait Strategy {
    fn distribution(&self) -> &DBRange;
    fn hasher(&self) -> &Hasher;
    fn get_key(&self, key: &RingSlice) -> Option<String>;
    fn tablename_len(&self) -> usize;
    fn write_database_table(&self, buf: &mut impl Write, key: &RingSlice);
}

struct Insert<'a, S> {
    strategy: &'a S,
    key: &'a RingSlice,
    val: RingSlice,
}
struct Update<'a, S> {
    strategy: &'a S,
    key: &'a RingSlice,
    val: RingSlice,
}
struct Select<'a, S> {
    strategy: &'a S,
    key: &'a RingSlice,
}
struct Delete<'a, S> {
    strategy: &'a S,
    key: &'a RingSlice,
}
//提前预留的转义后可能增长的长度，通常对于业务来说如果使用json格式，那么引号出现概率还是挺大的
const ESCAPED_GROW_LEN: usize = 8;
impl<'a, S: Strategy> Insert<'a, S> {
    fn len(&self) -> usize {
        // format!("insert into {dname}.{tname} (id, content) values ({key}, {val})")
        "insert into  (id,content) values (,)".len()
            + self.strategy.tablename_len()
            + self.key.len()
            + self.val.len()
            + ESCAPED_GROW_LEN
    }
    fn build(&self, packet: &mut PacketCodec) {
        let &Self { strategy, key, val } = self;
        packet.push_str("insert into ");
        strategy.write_database_table(packet, key);
        packet.push_str(" (id,content) values (");
        extend_escape_string(packet, key);
        packet.push_str(",'");
        extend_escape_string(packet, &val);
        packet.push_str("')")
    }
}
impl<'a, S: Strategy> Update<'a, S> {
    fn len(&self) -> usize {
        //update  set content= where id=
        "update  set content= where id=".len()
            + self.strategy.tablename_len()
            + self.key.len()
            + self.val.len()
            + ESCAPED_GROW_LEN
    }
    fn build(&self, packet: &mut PacketCodec) {
        let &Self { strategy, key, val } = self;
        //update  set content= where id=
        packet.push_str("update ");
        strategy.write_database_table(packet, key);
        packet.push_str(" set content='");
        extend_escape_string(packet, &val);
        packet.push_str("' where id=");
        extend_escape_string(packet, key);
    }
}
impl<'a, S: Strategy> Select<'a, S> {
    fn len(&self) -> usize {
        // format!("select content from {dname}.{tname} where id={key}")
        "select content from  where id=".len() + self.strategy.tablename_len() + self.key.len()
    }
    fn build(&self, packet: &mut PacketCodec) {
        let &Self { strategy, key } = self;
        packet.push_str("select content from ");
        strategy.write_database_table(packet, key);
        packet.push_str(" where id=");
        extend_escape_string(packet, key);
    }
}
impl<'a, S: Strategy> Delete<'a, S> {
    fn len(&self) -> usize {
        // delete from  where id=
        "delete from  where id=".len() + self.strategy.tablename_len() + self.key.len()
    }
    fn build(&self, packet: &mut PacketCodec) {
        let &Self { strategy, key } = self;
        packet.push_str("delete from ");
        strategy.write_database_table(packet, key);
        packet.push_str(" where id=");
        extend_escape_string(packet, key);
    }
}

enum SqlBuilder<'a, S> {
    Insert(Insert<'a, S>),
    Update(Update<'a, S>),
    Select(Select<'a, S>),
    Delete(Delete<'a, S>),
}
impl<'a, S: Strategy> SqlBuilder<'a, S> {
    fn len(&self) -> usize {
        match self {
            Self::Insert(i) => i.len(),
            Self::Update(i) => i.len(),
            Self::Select(i) => i.len(),
            Self::Delete(i) => i.len(),
        }
    }
    fn build(&self, packet: &mut PacketCodec) {
        match self {
            Self::Insert(i) => i.build(packet),
            Self::Update(i) => i.build(packet),
            Self::Select(i) => i.build(packet),
            Self::Delete(i) => i.build(packet),
        }
    }
}

//todo 这一块协议转换可整体移到protocol中，对Strategy的依赖可抽象
pub struct MysqlBuilder {}

// https://dev.mysql.com/doc/refman/8.0/en/string-literals.html
// Backslash (\) and the quote character used to quote the string must be escaped. In certain client environments, it may also be necessary to escape NUL or Control+Z.
// 应该只需要转义上面的
fn escape_mysql_and_push(packet: &mut PacketCodec, c: u8) {
    //非法char要当成二进制push，否则会变成unicode
    let c = c as char;
    if c == '\\' || c == '\'' || c == '"' {
        packet.push('\\' as u8);
        packet.push(c as u8);
    // } else if c == '\x00' {
    //     packet.push('\\' as u8);
    //     packet.push('0' as u8);
    // } else if c == '\n' {
    //     packet.push('\\' as u8);
    //     packet.push('n' as u8);
    // } else if c == '\r' {
    //     packet.push('\\' as u8);
    //     packet.push('r' as u8);
    // } else if c == '\x1a' {
    //     packet.push('\\' as u8);
    //     packet.push('Z' as u8);
    } else {
        packet.push(c as u8);
    }
}
fn extend_escape_string(packet: &mut PacketCodec, r: &RingSlice) {
    r.visit(|c| escape_mysql_and_push(packet, c))
}

impl MysqlBuilder {
    pub fn build_packets(
        &self,
        strategy: &impl Strategy,
        req: &HashedCommand,
        key: &RingSlice,
    ) -> Result<Vec<u8>> {
        let op = req.op();
        let sql_builder = match op {
            OP_ADD => SqlBuilder::Insert(Insert {
                strategy,
                key,
                val: req.value(),
            }),
            OP_SET => SqlBuilder::Update(Update {
                strategy,
                key,
                val: req.value(),
            }),
            OP_DEL => SqlBuilder::Delete(Delete { strategy, key }),
            OP_GET | OP_GETK => SqlBuilder::Select(Select { strategy, key }),
            //todo 返回原因
            _ => return Err(MysqlError(format!("not support op:{op}").into_bytes())),
        };

        let mut packet = PacketCodec::default();
        packet.reserve(sql_builder.len() + 5);
        //5即下面两行包头长度
        packet.write_next_packet_header();
        packet.push(req.mysql_cmd() as u8);

        sql_builder.build(&mut packet);

        packet.finish_current_packet();
        packet
            .check_total_payload_len()
            .map_err(|_| MysqlError("payload > max_allowed_packet".to_owned().into_bytes()))?;
        let packet: Vec<u8> = packet.into();
        log::debug!(
            "build mysql packet:{}",
            String::from_utf8(
                packet
                    .iter()
                    .map(|b| std::ascii::escape_default(*b))
                    .flatten()
                    .collect()
            )
            .unwrap()
        );
        Ok(packet)
    }
}
