use std::collections::HashMap;

use chrono_tz::Asia::Shanghai;
use ds::RingSlice;
use sharding::{distribution::DBRange, hash::Hasher};

use crate::mysql::strategy::replace_one;

use super::{
    strategy::{build_date_tname, to_i64, Strategy, TNamePostfixType},
    uuid::UuidHelper,
};
const DB_NAME_EXPRESSION: &str = "$db$";
const TABLE_NAME_EXPRESSION: &str = "$tb$";
const KEY_EXPRESSION: &str = "$k$";

#[derive(Debug, Clone, Debug, Clone, Default)]
pub struct KVTime {
    pub(crate) db_prefix: String,
    pub(crate) table_prefix: String,
    pub(crate) table_postfix: TNamePostfixType,
    pub(crate) db_count: u32,
    pub(crate) table_count: u32,
    pub(crate) sql: HashMap<String, String>,
    pub(crate) hasher: Hasher,
    pub(crate) distribution: DBRange,
}

impl KVTime {
    pub fn new(name: String, db_count: u32, shards: u32) -> Self {
        Self {
            db_prefix: name.clone(),
            table_prefix: name.clone(),
            table_postfix: TNamePostfixType::YYMMDD,
            db_count: db_count,
            table_count: 1,
            sql: HashMap::new(),
            distribution: DBRange::new(db_count as usize, 1usize, shards),
            hasher: Hasher::from("ddd"),
        }
    }
    fn build_tname(&self, key: &RingSlice) -> Option<String> {
        let postfix_type = self.table_postfix;
        let table_prefix = self.table_prefix.as_str();
        match postfix_type {
            TNamePostfixType::YYMM => {
                let tname = build_date_tname(table_prefix, key, false);
                tname
            }
            TNamePostfixType::YYMMDD => {
                let tname = build_date_tname(table_prefix, key, true);
                tname
            }
            TNamePostfixType::INDEX => {
                let tname = self.build_idx_tname(key);
                tname
            }
        }
    }

    fn build_dname(&self, key: &RingSlice) -> Option<String> {
        let dname_prefix = self.db_prefix.clone();
        let mut db_idx = 0;
        db_idx = self.distribution.db_idx(self.hasher.hash(key));
        return Some(format!("{}_{}", dname_prefix, db_idx));
    }
    fn build_idx_tname(&self, key: &RingSlice) -> Option<String> {
        let table_prefix = self.table_prefix.clone();
        if self.table_count > 0 && self.db_count > 0 {
            let mut tbl_index = 0;
            tbl_index = self.distribution.table_idx(self.hasher.hash(key));
            return Some(format!("{}_{}", table_prefix, tbl_index));
        } else {
            log::error!("id is null");
        }
        None
    }
}

impl Strategy for KVTime {
    fn distribution(&self) -> &DBRange {
        &self.distribution
    }
    fn hasher(&self) -> &Hasher {
        &self.hasher
    }

    fn get_key(&self, key: &RingSlice) -> String {
        let id = to_i64(key);
        let milliseconds = UuidHelper::get_time(id) * 1000;
        chrono::Utc
            .timestamp_millis(milliseconds)
            .with_timezone(&Shanghai)
            .format("%Y")
            .to_string()
    }

    //todo: sql_name 枚举
    fn build_sql(&self, sql_name: &str, db_key: &RingSlice, tbl_key: &RingSlice) -> Option<String> {
        let tname = match self.build_tname(tbl_key) {
            Some(tname) => tname,
            None => return None,
        };
        let dname = match self.build_dname(db_key) {
            Some(dname) => dname,
            None => return None,
        };
        let mut sql = self.sql.get(sql_name).unwrap_or(&String::new()).clone();
        if !sql.is_empty() {
            sql = sql.replace(DB_NAME_EXPRESSION, &dname);
            sql = sql.replace(TABLE_NAME_EXPRESSION, &tname);
            // TODO 先走通，再优化 fishermen
            sql = replace_one(&sql, KEY_EXPRESSION, db_key).expect("malformed sql");
        } else {
            log::error!("find the sql by name {} is empty or null", sql_name);
        }
        log::debug!("{}", sql);
        Some(sql)
    }
}
