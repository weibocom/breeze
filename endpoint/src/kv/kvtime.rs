use super::config::ARCHIVE_DEFAULT_KEY;
use super::{
    strategy::{to_i64, Postfix, Strategy},
    uuid::Uuid,
};
use chrono::TimeZone;
use chrono_tz::Asia::Shanghai;
use ds::RingSlice;
use protocol::kv::{Binary, Opcode};
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
    fn build_insert_sql(&self, dname: &str, tname: &str, key: &str, val: &str) -> String {
        format!("insert into {dname}.{tname} (id, content) values ({key}, {val})")
    }
    fn build_select_sql(&self, dname: &str, tname: &str, key: &str) -> String {
        format!("select content from {dname}.{tname} where id={key}")
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
        let sql = if op == Opcode::ADD as u8 {
            self.build_insert_sql(&dname, &tname, key.into(), (&req.value()).into())
        } else {
            self.build_select_sql(&dname, &tname, key.into())
        };
        log::debug!("{}", sql);
        Some(sql)
    }
}
