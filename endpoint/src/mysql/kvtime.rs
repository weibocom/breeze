use std::collections::HashMap;

use chrono::{TimeZone, Utc};
use chrono_tz::Asia::Shanghai;
use ds::RingSlice;
use sharding::hash::Hash;
use sharding::{distribution::DBRange, hash::Hasher};

use crate::mysql::strategy::replace_one;

use super::{
    strategy::{to_i64, Postfix, Strategy},
    uuid::UuidHelper,
};
const DB_NAME_EXPRESSION: &str = "$db$";
const TABLE_NAME_EXPRESSION: &str = "$tb$";
const KEY_EXPRESSION: &str = "$k$";

#[derive(Default, Clone, Debug)]
pub struct KVTime {
    db_prefix: String,
    table_prefix: String,
    table_postfix: Postfix,
    db_count: u32,
    table_count: u32,
    hasher: Hasher,
    distribution: DBRange,
}

impl KVTime {
    pub fn new(name: String, db_count: u32, shards: u32) -> Self {
        Self {
            db_prefix: name.clone(),
            table_prefix: name.clone(),
            table_postfix: Postfix::YYMMDD,
            db_count: db_count,
            table_count: 1,
            distribution: DBRange::new(db_count as usize, 1usize, shards as usize),
            hasher: Hasher::from("crc32"),
        }
    }
    fn build_tname(&self, key: &RingSlice) -> Option<String> {
        let table_prefix = self.table_prefix.as_str();
        match self.table_postfix {
            Postfix::YYMM => {
                let tname = self.build_date_tname(table_prefix, key, false);
                tname
            }
            Postfix::YYMMDD => {
                let tname = self.build_date_tname(table_prefix, key, true);
                tname
            }
            Postfix::INDEX => {
                let tname = self.build_idx_tname(key);
                tname
            }
        }
    }

    fn build_date_tname(
        &self,
        tbl_prefix: &str,
        key: &RingSlice,
        is_display_day: bool,
    ) -> Option<String> {
        // TODO key 转位i64，后面整合到类型parse的专门类中 fishermen
        let id = to_i64(key);

        let milliseconds = UuidHelper::get_time(id) * 1000;
        let yy_mm_dd = if is_display_day {
            chrono::Utc
                .timestamp_millis(milliseconds)
                .with_timezone(&Shanghai)
                .format("%y%m%d")
                .to_string()
        } else {
            chrono::Utc
                .timestamp_millis(milliseconds)
                .with_timezone(&Shanghai)
                .format("%y%m")
                .to_string()
        };
        log::debug!("with shanghai timezone:{} {}", key, yy_mm_dd);
        Some(format!("{}_{}", tbl_prefix, yy_mm_dd))
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
    fn build_ksql(&self, key: &RingSlice) -> Option<String> {
        let mut sql = "select content from $db$.$tb$ where id=$k$".to_string();
        let tname = match self.build_tname(key) {
            Some(tname) => tname,
            None => return None,
        };
        let dname = match self.build_dname(key) {
            Some(dname) => dname,
            None => return None,
        };
        if !sql.is_empty() {
            sql = sql.replace(DB_NAME_EXPRESSION, &dname);
            sql = sql.replace(TABLE_NAME_EXPRESSION, &tname);
            // TODO 先走通，再优化 fishermen
            sql = replace_one(&sql, KEY_EXPRESSION, key).expect("malformed sql");
        } else {
            log::error!("find the sql by name {} is empty or null", sql);
        }
        log::debug!("{}", sql);
        Some(sql)
    }
}
