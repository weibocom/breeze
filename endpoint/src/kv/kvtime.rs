use super::config::ARCHIVE_DEFAULT_KEY_U16;
use super::{
    strategy::{to_i64, Postfix, Strategy},
    uuid::Uuid,
};
use chrono::TimeZone;
use chrono_tz::Asia::Shanghai;
use ds::RingSlice;
use protocol::kv::{Binary, OP_ADD, OP_DEL, OP_GET, OP_GETK, OP_SET};
use protocol::kv::{MysqlBinary, PacketCodec};
use protocol::Error::FlushOnClose;
use protocol::HashedCommand;
use protocol::Result;
use sharding::hash::Hash;
use sharding::{distribution::DBRange, hash::Hasher};

#[derive(Default, Clone, Debug)]
pub struct KVTime {
    db_prefix: String,
    table_prefix: String,
    table_postfix: Postfix,
    hasher: Hasher,
    distribution: DBRange,
    years: Vec<u16>,
}

impl KVTime {
    pub fn new(name: String, db_count: u32, shards: u32, years: Vec<u16>) -> Self {
        Self {
            db_prefix: name.clone(),
            table_prefix: name.clone(),
            table_postfix: Postfix::YYMMDD,
            distribution: DBRange::new(db_count as usize, 1usize, shards as usize),
            hasher: Hasher::from("crc32"),
            years: years,
        }
    }
    fn build_tname(&self, uuid: i64) -> Option<String> {
        let table_prefix = self.table_prefix.as_str();
        match self.table_postfix {
            Postfix::YYMM => {
                let tname = self.build_date_tname(table_prefix, uuid, false);
                tname
            }
            //Postfix::YYMMDD
            _ => {
                let tname = self.build_date_tname(table_prefix, uuid, true);
                tname
            }
        }
    }

    fn build_date_tname(
        &self,
        tbl_prefix: &str,
        uuid: i64,
        is_display_day: bool,
    ) -> Option<String> {
        //todo uuid后面调整
        let secs = uuid.unix_secs();
        let yy_mm_dd = if is_display_day { "%y%m%d" } else { "%y%m" };
        let s = chrono::Utc
            .timestamp_opt(secs, 0)
            .unwrap()
            .with_timezone(&Shanghai)
            .format(yy_mm_dd)
            .to_string();
        log::debug!("with shanghai timezone:{} {}", uuid, s);
        Some(format!("{}_{}", tbl_prefix, s))
    }

    fn build_dname(&self, key: &RingSlice) -> Option<String> {
        //
        let db_idx: usize = self.distribution.db_idx(self.hasher.hash(key));
        return Some(format!("{}_{}", self.db_prefix, db_idx));
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

    // https://dev.mysql.com/doc/refman/8.0/en/string-literals.html
    // Backslash (\) and the quote character used to quote the string must be escaped. In certain client environments, it may also be necessary to escape NUL or Control+Z.
    // 应该只需要转义上面的
}

impl Strategy for KVTime {
    fn distribution(&self) -> &DBRange {
        &self.distribution
    }
    fn hasher(&self) -> &Hasher {
        &self.hasher
    }
    fn year(&self, key: &RingSlice) -> u16 {
        let uuid = to_i64(key);
        let year = uuid.year();

        if self.years.contains(&year) {
            year
        } else {
            ARCHIVE_DEFAULT_KEY_U16
        }
    }
    //todo: sql_name 枚举
    fn build_kvcmd(&self, req: &HashedCommand, key: RingSlice) -> Result<Vec<u8>> {
        let uuid = to_i64(&key);
        let tname = match self.build_tname(uuid) {
            Some(tname) => tname,
            None => return Err(FlushOnClose("build tname err".to_owned().into_bytes())),
        };
        let dname = match self.build_dname(&key) {
            Some(dname) => dname,
            None => return Err(FlushOnClose("build dname err".to_owned().into_bytes())),
        };

        MysqlBuilder::new(dname, tname, req).build_packets()
    }
}

//todo 这一块协议转换可整体移到protocol中，对Strategy的依赖可抽象
struct MysqlBuilder<'a> {
    dname: String,
    tname: String,
    req: &'a self::HashedCommand,
}

impl<'a> MysqlBuilder<'a> {
    fn new(dname: String, tname: String, req: &self::HashedCommand) -> MysqlBuilder {
        MysqlBuilder { dname, tname, req }
    }
    fn build_packets(&self) -> Result<Vec<u8>> {
        let mut packet = PacketCodec::default();
        packet.write_next_packet_header();
        packet.push(self.req.mysql_cmd() as u8);

        let op = self.req.op();
        let key = self.req.key();
        match op {
            OP_ADD => Self::build_insert_sql(&mut packet, &self.dname, &self.tname, self.req, &key),
            OP_SET => Self::build_update_sql(&mut packet, &self.dname, &self.tname, self.req, &key),
            OP_DEL => Self::build_delete_sql(&mut packet, &self.dname, &self.tname, self.req, &key),
            OP_GET | OP_GETK => {
                Self::build_select_sql(&mut packet, &self.dname, &self.tname, self.req, &key)
            }
            //todo 返回原因
            _ => return Err(FlushOnClose(format!("not support op:{op}").into_bytes())),
        };

        packet.finish_current_packet();
        packet
            .check_total_payload_len()
            .map_err(|_| FlushOnClose("payload > max_allowed_packet".to_owned().into_bytes()))?;
        Ok(packet.into())
    }
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
        dname: &str,
        tname: &str,
        req: &RingSlice,
        key: &RingSlice,
    ) {
        // format!("insert into {dname}.{tname} (id, content) values ({key}, {val})")
        let val = req.value();

        let len = "insert into . (id,content) values (,)".len()
            + dname.len()
            + tname.len()
            + key.len()
            + val.len();
        packet.reserve(len);
        packet.push_str("insert into ");
        packet.push_str(dname);
        packet.push('.' as u8);
        packet.push_str(tname);
        packet.push_str(" (id,content) values (");
        Self::extend_escape_string(packet, key);
        packet.push_str(",'");
        Self::extend_escape_string(packet, &val);
        packet.push_str("')");
    }
    fn build_update_sql(
        packet: &mut PacketCodec,
        dname: &str,
        tname: &str,
        req: &RingSlice,
        key: &RingSlice,
    ) {
        //update . set content= where id=
        let val = req.value();

        let len = "update . set content= where id=".len()
            + dname.len()
            + tname.len()
            + key.len()
            + val.len();
        packet.reserve(len);
        packet.push_str("update ");
        packet.push_str(dname);
        packet.push('.' as u8);
        packet.push_str(tname);
        packet.push_str(" set content='");
        Self::extend_escape_string(packet, &val);
        packet.push_str("' where id=");
        Self::extend_escape_string(packet, key);
    }
    fn build_select_sql(
        packet: &mut PacketCodec,
        dname: &str,
        tname: &str,
        _req: &RingSlice,
        key: &RingSlice,
    ) {
        // format!("select content from {dname}.{tname} where id={key}")
        packet.reserve(128);
        packet.push_str("select content from ");
        packet.push_str(dname);
        packet.push('.' as u8);
        packet.push_str(tname);
        packet.push_str(" where id=");
        Self::extend_escape_string(packet, key);
    }
    fn build_delete_sql(
        packet: &mut PacketCodec,
        dname: &str,
        tname: &str,
        _req: &RingSlice,
        key: &RingSlice,
    ) {
        // format!("select content from {dname}.{tname} where id={key}")
        // delete from . where id=
        packet.reserve(64);
        packet.push_str("delete from ");
        packet.push_str(dname);
        packet.push('.' as u8);
        packet.push_str(tname);
        packet.push_str(" where id=");
        Self::extend_escape_string(packet, key);
    }
}
