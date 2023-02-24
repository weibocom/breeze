use super::config::MysqlNamespace;
use super::topo::MysqlService;
use super::uuid::UuidHelper;
use chrono::{DateTime, TimeZone, Utc};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::SystemTime;
use std::time::{Duration, UNIX_EPOCH};

const DB_NAME_EXPRESSION: &str = "$db$";
const TABLE_NAME_EXPRESSION: &str = "$tb$";

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

pub struct table;
impl table {
    pub fn get_sql(
        item: &MysqlNamespace,
        sql_name: &str,
        db_id: i64,
        tb_id: i64,
    ) -> Option<String> {
        let table_name = match Self::get_table_name(item, tb_id) {
            Some(table_name) => table_name,
            None => return None,
        };
        let db_name = match Self::get_db_name_by_id(item, db_id) {
            Some(db_name) => db_name,
            None => return None,
        };
        let mut sql = item.sql.get(sql_name).unwrap_or(&String::new()).clone();
        if !sql.is_empty() {
            sql = sql.replace(DB_NAME_EXPRESSION, &db_name);
            sql = sql.replace(TABLE_NAME_EXPRESSION, &table_name);
        } else {
            log::error!("find the sql by name {} is empty or null", sql_name);
        }
        log::debug!("{}", sql);
        Some(sql)
    }

    pub fn get_table_name(item: &MysqlNamespace, id: i64) -> Option<String> {
        let postfix_type = if item.basic.table_postfix == "yymmdd" {
            TNamePostfixType::YYMMDD
        } else if item.basic.table_postfix == "yymm" {
            TNamePostfixType::YYMM
        } else {
            TNamePostfixType::INDEX
        };
        let table_prefix = item.basic.table_prefix.as_str();
        match postfix_type {
            TNamePostfixType::YYMM => {
                let table_name = Self::get_date_table_name_by_id(table_prefix, id, false);
                table_name
            }
            TNamePostfixType::YYMMDD => {
                let table_name = Self::get_date_table_name_by_id(table_prefix, id, true);
                table_name
            }
            TNamePostfixType::INDEX => {
                let table_name = Self::get_index_table_name_by_id(item, id);
                table_name
            }
        }
    }

    fn get_db_name_by_id(item: &MysqlNamespace, id: i64) -> Option<String> {
        let (db_name_prefix, db_count) = (item.basic.db_prefix.clone(), item.basic.db_count);
        //todo: check db_name_prefix not empty
        if let id = id {
            let db_index = 0;
            // let db_index = api_util::get_hash4split(id, db_count * item.table_count.max(1));
            let db_index = db_index / item.basic.table_count;
            return Some(format!("{}_{}", db_name_prefix, db_index));
        } else {
            log::error!("id is null");
        }
        None
    }
    fn get_index_table_name_by_id(item: &MysqlNamespace, id: i64) -> Option<String> {
        let (table_prefix, id) = (item.basic.table_prefix.clone(), id);
        //todo: check db_name_prefix not empty
        if item.basic.table_count > 0 && item.basic.db_count > 0 {
            let mut tbl_index = 0;
            // tbl_index = api_util::get_hash4split(id, item.db_count * item.table_count);
            tbl_index = tbl_index % item.basic.table_count;
            return Some(format!("{}_{}", table_prefix, tbl_index));
        } else {
            log::error!("id is null");
        }
        None
    }
    fn get_date_table_name_by_id(
        tbl_prefix: &str,
        id: i64,
        is_display_day: bool,
    ) -> Option<String> {
        if let id_val = id {
            let milliseconds = UuidHelper::get_time_from_id(id_val) * 1000;
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
        } else {
            log::error!("tblPrefix is null or empty, id is null");
            None
        }
    }

    pub fn get_sql_key(db_name: &str, table_name: &str, sql_name: &str) -> String {
        format!("{}.{}.{}", db_name, table_name, sql_name)
    }
}
