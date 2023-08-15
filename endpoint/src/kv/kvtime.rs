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
        let (year, month, day) = (s.year(), s.month(), s.day());
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

//todo 这一块协议转换可整体移到protocol中，对Strategy的依赖可抽象
pub struct MysqlBuilder {}

impl MysqlBuilder {
    pub fn build_packets(
        &self,
        strategy: &impl Strategy,
        req: &self::HashedCommand,
        key: &RingSlice,
    ) -> Result<Vec<u8>> {
        let mut packet = PacketCodec::default();
        packet.write_next_packet_header();
        packet.push(req.mysql_cmd() as u8);

        let op = req.op();
        match op {
            OP_ADD => Self::build_insert_sql(&mut packet, strategy, req, &key),
            OP_SET => Self::build_update_sql(&mut packet, strategy, req, &key),
            OP_DEL => Self::build_delete_sql(&mut packet, strategy, req, &key),
            OP_GET | OP_GETK => Self::build_select_sql(&mut packet, strategy, req, &key),
            //todo 返回原因
            _ => return Err(MysqlError(format!("not support op:{op}").into_bytes())),
        };

        packet.finish_current_packet();
        packet
            .check_total_payload_len()
            .map_err(|_| MysqlError("payload > max_allowed_packet".to_owned().into_bytes()))?;
        Ok(packet.into())
    }
    // https://dev.mysql.com/doc/refman/8.0/en/string-literals.html
    // Backslash (\) and the quote character used to quote the string must be escaped. In certain client environments, it may also be necessary to escape NUL or Control+Z.
    // 应该只需要转义上面的
    fn escape_mysql_and_push(packet: &mut PacketCodec, c: u8) {
        //非法char要当成二进制push，否则会变成unicode
        let c = c as char;
        if c == '\x00' {
            packet.push('\\' as u8);
            packet.push('0' as u8);
        } else if c == '\n' {
            packet.push('\\' as u8);
            packet.push('n' as u8);
        } else if c == '\r' {
            packet.push('\\' as u8);
            packet.push('r' as u8);
        } else if c == '\\' || c == '\'' || c == '"' {
            packet.push('\\' as u8);
            packet.push(c as u8);
        } else if c == '\x1a' {
            packet.push('\\' as u8);
            packet.push('Z' as u8);
        } else {
            packet.push(c as u8);
        }
    }
    fn extend_escape_string(packet: &mut PacketCodec, r: &RingSlice) {
        r.visit(|c| Self::escape_mysql_and_push(packet, c))
    }
    fn build_insert_sql(
        packet: &mut PacketCodec,
        strategy: &impl Strategy,
        req: &RingSlice,
        key: &RingSlice,
    ) {
        // format!("insert into {dname}.{tname} (id, content) values ({key}, {val})")
        let val = req.value();

        let len = "insert into  (id,content) values (,)".len()
            + strategy.tablename_len()
            + key.len()
            + val.len();
        packet.reserve(len);
        packet.push_str("insert into ");
        strategy.write_database_table(packet, key);
        packet.push_str(" (id,content) values (");
        Self::extend_escape_string(packet, key);
        packet.push_str(",'");
        Self::extend_escape_string(packet, &val);
        packet.push_str("')");
    }
    fn build_update_sql(
        packet: &mut PacketCodec,
        strategy: &impl Strategy,
        req: &RingSlice,
        key: &RingSlice,
    ) {
        //update  set content= where id=
        let val = req.value();

        let len = "update  set content= where id=".len()
            + strategy.tablename_len()
            + key.len()
            + val.len();
        packet.reserve(len);
        packet.push_str("update ");
        strategy.write_database_table(packet, key);
        packet.push_str(" set content='");
        Self::extend_escape_string(packet, &val);
        packet.push_str("' where id=");
        Self::extend_escape_string(packet, key);
    }
    fn build_select_sql(
        packet: &mut PacketCodec,
        strategy: &impl Strategy,
        _req: &RingSlice,
        key: &RingSlice,
    ) {
        // format!("select content from {dname}.{tname} where id={key}")
        packet.reserve(128);
        packet.push_str("select content from ");
        strategy.write_database_table(packet, key);
        packet.push_str(" where id=");
        Self::extend_escape_string(packet, key);
    }
    fn build_delete_sql(
        packet: &mut PacketCodec,
        strategy: &impl Strategy,
        _req: &RingSlice,
        key: &RingSlice,
    ) {
        // delete from  where id=
        packet.reserve(64);
        packet.push_str("delete from ");
        strategy.write_database_table(packet, key);
        packet.push_str(" where id=");
        Self::extend_escape_string(packet, key);
    }
}
