use super::config::MysqlNamespace;
use super::uuid::UuidHelper;
use chrono::{TimeZone, Utc};
use chrono_tz::Asia::Shanghai;
use ds::RingSlice;
use protocol::Result;

use sharding::distribution::DBRange;
use sharding::hash::{Hash, Hasher};
use std::collections::HashMap;

const DB_NAME_EXPRESSION: &str = "$db$";
const TABLE_NAME_EXPRESSION: &str = "$tb$";
const KEY_EXPRESSION: &str = "$k$";

pub enum TNamePostfixType {
    YYMM,
    YYMMDD,
    INDEX,
}

impl TNamePostfixType {
    pub fn value(&self) -> i32 {
        match self {
            TNamePostfixType::YYMM => 0,
            TNamePostfixType::YYMMDD => 1,
            TNamePostfixType::INDEX => 2,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Strategy {
    pub(crate) db_prefix: String,
    pub(crate) table_prefix: String,
    pub(crate) table_postfix: String,
    pub(crate) db_count: u32,
    pub(crate) table_count: u32,
    pub(crate) hierarchy: bool,
    pub(crate) sql: HashMap<String, String>,
    pub(crate) hasher: Hasher,
    pub(crate) distribution: DBRange,
}

impl Strategy {
    pub fn new(
        db_prefix: String,
        table_prefix: String,
        table_postfix: String,
        db_count: u32,
        table_count: u32,
        shards: u32,
        hierarchy: bool,
        sql: HashMap<String, String>,
        h: String,
        dist: String,
    ) -> Self {
        Self {
            db_prefix: db_prefix.clone(),
            table_prefix: table_prefix.clone(),
            table_postfix: table_postfix.clone(),
            db_count: db_count,
            table_count: table_count,
            hierarchy: hierarchy,
            sql: sql.clone(),
            distribution: DBRange::new(db_count as usize, table_count as usize, shards as usize),
            hasher: Hasher::from(h.as_str()),
        }
    }

    pub fn try_from(item: &MysqlNamespace) -> Self {
        Self {
            db_prefix: item.basic.db_prefix.clone(),
            table_prefix: item.basic.table_prefix.clone(),
            table_postfix: item.basic.table_postfix.clone(),
            db_count: item.basic.db_count,
            table_count: item.basic.table_count,
            hierarchy: item.basic.hierarchy,
            sql: item.sql.clone(),
            distribution: DBRange::new(
                item.basic.db_count as usize,
                item.basic.table_count as usize,
                item.backends.len() as usize,
            ),
            hasher: Hasher::from(item.basic.hash.as_str()),
        }
    }
    pub fn distribution(&self) -> &DBRange {
        &self.distribution
    }
    pub fn hasher(&self) -> &Hasher {
        &self.hasher
    }

    pub fn get_year(&self, key: &RingSlice) -> String {
        let id = to_i64(key);
        let milliseconds = UuidHelper::get_time(id) * 1000;
        chrono::Utc
            .timestamp_millis(milliseconds)
            .with_timezone(&Shanghai)
            .format("%Y")
            .to_string()
    }

    //todo: sql_name 枚举
    pub fn build_sql(
        &self,
        sql_name: &str,
        db_key: &RingSlice,
        tbl_key: &RingSlice,
    ) -> Option<String> {
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
            // sql = sql.replace(KEY_EXPRESSION, did.to_string().as_str());
        } else {
            log::error!("find the sql by name {} is empty or null", sql_name);
        }
        log::debug!("{}", sql);
        Some(sql)
    }

    fn build_tname(&self, key: &RingSlice) -> Option<String> {
        let postfix_type = if self.table_postfix == "yymmdd" {
            TNamePostfixType::YYMMDD
        } else if self.table_postfix == "yymm" {
            TNamePostfixType::YYMM
        } else {
            TNamePostfixType::INDEX
        };
        let table_prefix = self.table_prefix.as_str();
        match postfix_type {
            TNamePostfixType::YYMM => {
                let tname = self.build_date_tname(table_prefix, key, false);
                tname
            }
            TNamePostfixType::YYMMDD => {
                let tname = self.build_date_tname(table_prefix, key, true);
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
        //todo: check db_name_prefix not empty
        let mut db_idx = 0;
        db_idx = self.distribution.db_idx(self.hasher.hash(key));
        // let db_index = api_util::get_hash4split(id, db_count * item.table_count.max(1));
        // let db_idx = db_idx / self.table_count as usize;
        return Some(format!("{}_{}", dname_prefix, db_idx));
    }
    fn build_idx_tname(&self, key: &RingSlice) -> Option<String> {
        let table_prefix = self.table_prefix.clone();
        //todo: check db_name_prefix not empty
        if self.table_count > 0 && self.db_count > 0 {
            let mut tbl_index = 0;
            tbl_index = self.distribution.table_idx(self.hasher.hash(key));
            // tbl_index = api_util::get_hash4split(id, item.db_count * item.table_count);
            // tbl_index = tbl_index % self.table_count as usize;
            return Some(format!("{}_{}", table_prefix, tbl_index));
        } else {
            log::error!("id is null");
        }
        None
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

    pub fn build_sql_key(db_name: &str, table_name: &str, sql_name: &str) -> String {
        format!("{}.{}.{}", db_name, table_name, sql_name)
    }
}

fn replace_one(raw_sql: &String, from: &'static str, to: &RingSlice) -> Result<String> {
    match raw_sql.find(from) {
        Some(start) => {
            let end = start + from.len();
            Ok(format!(
                "{}{}{}",
                raw_sql.get(0..start).unwrap(),
                to_i64(to),
                raw_sql.get(end..).unwrap()
            ))
        }
        None => Err(protocol::Error::ResponseProtocolInvalid),
    }
}

fn to_i64(key: &RingSlice) -> i64 {
    log::debug!("+++ key: {:?}", key);
    let mut id = 0_i64;
    const ZERO: i64 = '0' as i64;
    for i in 0..key.len() {
        let c = key.at(i);
        assert!(c.is_ascii_digit());
        id = id * 10 + (c as i64) - ZERO;
    }
    id
}
