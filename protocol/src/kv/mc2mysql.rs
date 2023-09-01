use core::fmt::Write;
use enum_dispatch::enum_dispatch;
use std::fmt::Display;

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

struct TableDisplay<'a, S> {
    strategy: &'a S,
    key: &'a RingSlice,
}

impl<'a, S: Strategy> Display for TableDisplay<'a, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.strategy.write_database_table(f, self.key);
        Ok(())
    }
}

impl<'a, S> TableDisplay<'a, S> {
    fn wrap_strategy(strategy: &'a S, key: &'a RingSlice) -> Self {
        Self { strategy, key }
    }
}

struct KeyValDisplay<'a>(&'a RingSlice);
impl<'a> Display for KeyValDisplay<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.visit(|c| escape_mysql_and_push(f, c));
        Ok(())
    }
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
        "insert into  (id,content) values (,'')".len()
            + self.strategy.tablename_len()
            + self.key.len()
            + self.val.len()
            + ESCAPED_GROW_LEN
    }
    fn build(&self, packet: &mut PacketCodec) {
        let &Self { strategy, key, val } = self;
        let _ = write!(
            packet,
            "insert into {} (id,content) values ({},'{}')",
            TableDisplay::wrap_strategy(strategy, key),
            KeyValDisplay(key),
            KeyValDisplay(&val)
        );
    }
}
impl<'a, S: Strategy> Update<'a, S> {
    fn len(&self) -> usize {
        "update  set content='' where id=".len()
            + self.strategy.tablename_len()
            + self.key.len()
            + self.val.len()
            + ESCAPED_GROW_LEN
    }
    fn build(&self, packet: &mut PacketCodec) {
        let &Self { strategy, key, val } = self;
        let _ = write!(
            packet,
            "update {} set content='{}' where id={}",
            TableDisplay::wrap_strategy(strategy, key),
            KeyValDisplay(&val),
            KeyValDisplay(key)
        );
    }
}
impl<'a, S: Strategy> Select<'a, S> {
    fn len(&self) -> usize {
        // format!("select content from {dname}.{tname} where id={key}")
        "select content from  where id=".len() + self.strategy.tablename_len() + self.key.len()
    }
    fn build(&self, packet: &mut PacketCodec) {
        let &Self { strategy, key } = self;
        let _ = write!(
            packet,
            "select content from {} where id={}",
            TableDisplay::wrap_strategy(strategy, key),
            KeyValDisplay(key)
        );
    }
}
impl<'a, S: Strategy> Delete<'a, S> {
    fn len(&self) -> usize {
        "delete from  where id=".len() + self.strategy.tablename_len() + self.key.len()
    }
    fn build(&self, packet: &mut PacketCodec) {
        let &Self { strategy, key } = self;
        let _ = write!(
            packet,
            "delete from {} where id={}",
            TableDisplay::wrap_strategy(strategy, key),
            KeyValDisplay(key)
        );
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
// 应该只需要转义上面的，我们使用单引号，所以双引号也不转义了
fn escape_mysql_and_push(packet: &mut impl Write, c: u8) {
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
