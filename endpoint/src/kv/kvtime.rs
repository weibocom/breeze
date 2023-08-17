use super::config::ARCHIVE_DEFAULT_KEY;
use super::{
    strategy::{to_i64, Postfix, Strategy},
    uuid::Uuid,
};
use chrono::{Datelike, TimeZone};
use chrono_tz::Asia::Shanghai;
use core::fmt::Write;
use ds::RingSlice;
use protocol::kv::{Binary, OP_ADD, OP_DEL, OP_GET, OP_GETK, OP_SET};
use protocol::kv::{MysqlBinary, PacketCodec};
use protocol::HashedCommand;
use protocol::{Error::MysqlError, Result};
use sharding::hash::Hash;
use sharding::{distribution::DBRange, hash::Hasher};

#[derive(Default, Clone, Debug)]
pub struct KVTime {
    db_prefix: String,
    table_prefix: String,
    table_postfix: Postfix,
    hasher: Hasher,
    distribution: DBRange,
    years: Vec<String>,
}

impl KVTime {
    pub fn new(name: String, db_count: u32, shards: u32, years: Vec<String>) -> Self {
        Self {
            db_prefix: name.clone(),
            table_prefix: name.clone(),
            table_postfix: Postfix::YYMMDD,
            distribution: DBRange::new(db_count as usize, 1usize, shards as usize),
            hasher: Hasher::from("crc32"),
            years: years,
        }
    }
    fn write_date_tname(&self, buf: &mut impl Write, uuid: i64, is_display_day: bool) {
        //todo uuid后面调整
        let secs = uuid.unix_secs();
        let s = chrono::Utc
            .timestamp_opt(secs, 0)
            .unwrap()
            .with_timezone(&Shanghai);
        let (year, month, day) = (s.year() % 100, s.month(), s.day());
        if is_display_day {
            let _ = write!(buf, "{}_{}{:02}{:02}", &self.table_prefix, year, month, day);
        } else {
            let _ = write!(buf, "{}_{}{:02}", &self.table_prefix, year, month);
        }
    }

    fn write_tname(&self, buf: &mut impl Write, key: &RingSlice) {
        let uuid = to_i64(key);
        match self.table_postfix {
            Postfix::YYMM => {
                self.write_date_tname(buf, uuid, false);
            }
            //Postfix::YYMMDD
            _ => {
                self.write_date_tname(buf, uuid, true);
            }
        }
    }

    fn write_dname(&self, buf: &mut impl Write, key: &RingSlice) {
        let db_idx: usize = self.distribution.db_idx(self.hasher.hash(key));
        let _ = write!(buf, "{}_{}", self.db_prefix, db_idx);
    }
    // fn build_idx_tname(&self, key: &RingSlice) -> Option<String> {
    //     let table_prefix = self.table_prefix.clone();
    //     if self.table_count > 0 && self.db_count > 0 {
    //         let mut tbl_index = 0;
    //         tbl_index = self.distribution.table_idx(self.hasher.hash(key));
    //         return Some(format!("{}_{}", table_prefix, tbl_index));
    //     } else {
    //         log::error!("id is null");
    //     }
    //     None
    // }
}
impl Strategy for KVTime {
    fn distribution(&self) -> &DBRange {
        &self.distribution
    }
    fn hasher(&self) -> &Hasher {
        &self.hasher
    }
    fn get_key(&self, key: &RingSlice) -> Option<String> {
        let uuid = to_i64(key);
        let s = uuid.unix_secs();
        let year = chrono::Utc
            .timestamp_opt(s, 0)
            .unwrap()
            .with_timezone(&Shanghai)
            .format("%Y")
            .to_string();
        if self.years.contains(&year) {
            Some(year)
        } else {
            Some(ARCHIVE_DEFAULT_KEY.to_string())
        }
    }
    fn tablename_len(&self) -> usize {
        // status_6.status_030926, 8代表除去前缀后的长度
        self.db_prefix.len() + self.table_prefix.len() + 10
    }
    fn write_database_table(&self, buf: &mut impl Write, key: &RingSlice) {
        self.write_dname(buf, key);
        //这里是按照utf8编码写入的
        let _ = buf.write_char('.');
        self.write_tname(buf, key);
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
struct Selete<'a, S> {
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
impl<'a, S: Strategy> Selete<'a, S> {
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
    Selete(Selete<'a, S>),
    Delete(Delete<'a, S>),
}
impl<'a, S: Strategy> SqlBuilder<'a, S> {
    fn len(&self) -> usize {
        match self {
            Self::Insert(i) => i.len(),
            Self::Update(i) => i.len(),
            Self::Selete(i) => i.len(),
            Self::Delete(i) => i.len(),
        }
    }
    fn build(&self, packet: &mut PacketCodec) {
        match self {
            Self::Insert(i) => i.build(packet),
            Self::Update(i) => i.build(packet),
            Self::Selete(i) => i.build(packet),
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
        req: &self::HashedCommand,
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
            OP_GET | OP_GETK => SqlBuilder::Selete(Selete { strategy, key }),
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
