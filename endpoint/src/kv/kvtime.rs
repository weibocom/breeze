use super::config::ARCHIVE_DEFAULT_KEY;
use super::{
    strategy::{to_i64, Postfix, Strategy},
    uuid::Uuid,
};
use chrono::TimeZone;
use chrono_tz::Asia::Shanghai;
use ds::RingSlice;
use protocol::kv::{Binary, OP_ADD};
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
    fn escape_mysql_and_push(s: &mut String, c: u8) {
        //非法char要当成二进制push，否则会变成unicode
        let s = unsafe { s.as_mut_vec() };
        let c = c as char;
        if c == '\x00' {
            s.push('\\' as u8);
            s.push('0' as u8);
        } else if c == '\n' {
            s.push('\\' as u8);
            s.push('n' as u8);
        } else if c == '\r' {
            s.push('\\' as u8);
            s.push('r' as u8);
        } else if c == '\\' || c == '\'' || c == '"' {
            s.push('\\' as u8);
            s.push(c as u8);
        } else if c == '\x1a' {
            s.push('\\' as u8);
            s.push('Z' as u8);
        } else {
            s.push(c as u8);
        }
    }
    fn extend_escape_string(s: &mut String, r: &RingSlice) {
        r.visit(|c| Self::escape_mysql_and_push(s, c))
    }
    fn build_insert_sql(
        &self,
        dname: &str,
        tname: &str,
        req: &RingSlice,
        key: &RingSlice,
    ) -> String {
        // format!("insert into {dname}.{tname} (id, content) values ({key}, {val})")
        let val = req.value();

        let len = "insert into . (id,content) values (,)".len()
            + dname.len()
            + tname.len()
            + key.len()
            + val.len();
        let mut sql = String::with_capacity(len);
        sql.push_str("insert into ");
        sql.push_str(dname);
        sql.push('.');
        sql.push_str(tname);
        sql.push_str(" (id,content) values (");
        Self::extend_escape_string(&mut sql, key);
        sql.push_str(",'");
        Self::extend_escape_string(&mut sql, &val);
        sql.push_str("')");
        sql
    }
    fn build_select_sql(&self, dname: &str, tname: &str, key: &RingSlice) -> String {
        // format!("select content from {dname}.{tname} where id={key}")
        let mut sql = String::with_capacity(128);
        sql.push_str("select content from ");
        sql.push_str(dname);
        sql.push('.');
        sql.push_str(tname);
        sql.push_str(" where id=");
        Self::extend_escape_string(&mut sql, key);
        sql
    }
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
    //todo: sql_name 枚举
    fn build_kvsql(&self, req: &RingSlice, key: &RingSlice) -> Option<String> {
        let uuid = to_i64(key);
        let tname = match self.build_tname(uuid) {
            Some(tname) => tname,
            None => return None,
        };
        let dname = match self.build_dname(key) {
            Some(dname) => dname,
            None => return None,
        };

        let op = req.op();
        let sql = match op {
            OP_ADD => self.build_insert_sql(&dname, &tname, req, key),
            _ => self.build_select_sql(&dname, &tname, key),
        };
        log::debug!("{}", sql);
        Some(sql)
    }
}
