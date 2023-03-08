use super::config::MysqlNamespace;
use super::topo::MysqlService;
use super::uuid::UuidHelper;
use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use sharding::distribution::Distribute;
use sharding::hash::{Hash, HashKey, Hasher};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::SystemTime;
use std::time::{Duration, UNIX_EPOCH};

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
    pub(crate) distribution: Distribute,
}

impl Strategy {
    pub fn new(
        db_prefix: String,
        table_prefix: String,
        table_postfix: String,
        db_count: u32,
        table_count: u32,
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
            distribution: Distribute::from_num(dist.as_str(), (db_count * table_count) as usize),
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
            distribution: Distribute::from_num(
                item.basic.distribution.as_str(),
                (item.basic.db_count * item.basic.table_count) as usize,
            ),
            hasher: Hasher::from(item.basic.hash.as_str()),
        }
    }
    pub fn distribution(&self) -> &Distribute {
        &self.distribution
    }
    pub fn hasher(&self) -> &Hasher {
        &self.hasher
    }

    //todo: sql_name 枚举
    pub fn build_sql(&self, sql_name: &str, did: i64, tid: i64) -> Option<String> {
        let tname = match self.build_tname(tid) {
            Some(tname) => tname,
            None => return None,
        };
        let dname = match self.build_dname(did) {
            Some(dname) => dname,
            None => return None,
        };
        let mut sql = self.sql.get(sql_name).unwrap_or(&String::new()).clone();
        if !sql.is_empty() {
            sql = sql.replace(DB_NAME_EXPRESSION, &dname);
            sql = sql.replace(TABLE_NAME_EXPRESSION, &tname);
            sql = sql.replace(KEY_EXPRESSION, did.to_string().as_str());
        } else {
            log::error!("find the sql by name {} is empty or null", sql_name);
        }
        log::debug!("{}", sql);
        Some(sql)
    }

    pub fn build_tname(&self, id: i64) -> Option<String> {
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
                let tname = self.build_date_tname(table_prefix, id, false);
                tname
            }
            TNamePostfixType::YYMMDD => {
                let tname = self.build_date_tname(table_prefix, id, true);
                tname
            }
            TNamePostfixType::INDEX => {
                let tname = self.build_idx_tname(id);
                tname
            }
        }
    }

    fn build_dname(&self, id: i64) -> Option<String> {
        let dname_prefix = self.db_prefix.clone();
        //todo: check db_name_prefix not empty
        let mut db_idx = 0;
        db_idx = self
            .distribution
            .index(self.hasher.hash(&id.to_string().as_bytes()));
        // let db_index = api_util::get_hash4split(id, db_count * item.table_count.max(1));
        let db_idx = db_idx / self.table_count as usize;
        return Some(format!("{}_{}", dname_prefix, db_idx));
    }
    fn build_idx_tname(&self, id: i64) -> Option<String> {
        let table_prefix = self.table_prefix.clone();
        //todo: check db_name_prefix not empty
        if self.table_count > 0 && self.db_count > 0 {
            let mut tbl_index = 0;
            tbl_index = self
                .distribution
                .index(self.hasher.hash(&id.to_string().as_bytes()));
            // tbl_index = api_util::get_hash4split(id, item.db_count * item.table_count);
            tbl_index = tbl_index % self.table_count as usize;
            return Some(format!("{}_{}", table_prefix, tbl_index));
        } else {
            log::error!("id is null");
        }
        None
    }
    fn build_date_tname(&self, tbl_prefix: &str, id: i64, is_display_day: bool) -> Option<String> {
        let milliseconds = UuidHelper::get_time(id) * 1000;
        let yy_mm_dd = if is_display_day {
            chrono::Utc
                .timestamp_millis(milliseconds)
                .format("%y%m%d")
                .to_string()
        } else {
            chrono::Utc
                .timestamp_millis(milliseconds)
                .format("%y%m")
                .to_string()
        };
        Some(format!("{}_{}", tbl_prefix, yy_mm_dd))
    }

    pub fn build_sql_key(db_name: &str, table_name: &str, sql_name: &str) -> String {
        format!("{}.{}.{}", db_name, table_name, sql_name)
    }
}
