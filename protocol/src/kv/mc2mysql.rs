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

const ESCAPED_GROW_LEN: usize = 8;
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
        let sql_len = match op {
            OP_ADD => {
                "insert into  (id,content) values (,'')".len()
                    + strategy.tablename_len()
                    + key.len()
                    + req.value().len()
                    + ESCAPED_GROW_LEN
            }
            OP_SET => {
                "update  set content='' where id=".len()
                    + strategy.tablename_len()
                    + key.len()
                    + req.value().len()
                    + ESCAPED_GROW_LEN
            }
            OP_DEL => "delete from  where id=".len() + strategy.tablename_len() + key.len(),
            OP_GET | OP_GETK => {
                "select content from  where id=".len() + strategy.tablename_len() + key.len()
            }
            //todo 返回原因
            _ => return Err(MysqlError(format!("not support op:{op}").into_bytes())),
        };

        let mut packet = PacketCodec::default();
        packet.reserve(sql_len + 5);
        //5即下面两行包头长度
        packet.write_next_packet_header();
        packet.push(req.mysql_cmd() as u8);

        match op {
            OP_ADD => {
                let _ = write!(
                    packet,
                    "insert into {} (id,content) values ({},'{}')",
                    TableDisplay::wrap_strategy(strategy, key),
                    KeyValDisplay(key),
                    KeyValDisplay(&req.value())
                );
            }
            OP_SET => {
                let _ = write!(
                    packet,
                    "update {} set content='{}' where id={}",
                    TableDisplay::wrap_strategy(strategy, key),
                    KeyValDisplay(&req.value()),
                    KeyValDisplay(key)
                );
            }
            OP_DEL => {
                let _ = write!(
                    packet,
                    "delete from {} where id={}",
                    TableDisplay::wrap_strategy(strategy, key),
                    KeyValDisplay(key)
                );
            }
            OP_GET | OP_GETK => {
                let _ = write!(
                    packet,
                    "select content from {} where id={}",
                    TableDisplay::wrap_strategy(strategy, key),
                    KeyValDisplay(key)
                );
            }
            //todo 返回原因
            _ => return Err(MysqlError(format!("not support op:{op}").into_bytes())),
        };

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
