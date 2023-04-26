use super::config::MysqlNamespace;
use super::kvtime::KVTime;
use super::uuid::UuidHelper;
use chrono::{TimeZone, Utc};
use chrono_tz::Asia::Shanghai;
use ds::RingSlice;
use protocol::{Error, Result};

use sharding::distribution::DBRange;
use sharding::hash::{Hash, Hasher};
use std::collections::HashMap;
const ARCHIVE_SHARDS_KEY: &str = "__default__";

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

pub trait Strategy {
    fn distribution(&self) -> &DBRange;
    fn hasher(&self) -> &Hasher;
    fn get_key(&self, key: &RingSlice) -> String;
    fn build_sql(&self, key: &RingSlice) -> Option<String>;
}

pub enum Strategyer {
    KVTime(KVTime),
}

impl Strategyer {
    pub fn try_from(item: &MysqlNamespace) -> Self {
        // item.basic.strategy

        Self::KVTime(KVTime::new(
            item.basic.table_name,
            item.basic.db_count,
            item.backends
                .get("ARCHIVE_SHARDS_KEY")
                .unwrap_or_default()
                .len(),
        ))
    }
}
//     pub fn new(
//         db_prefix: String,
//         table_prefix: String,
//         table_postfix: String,
//         db_count: u32,
//         table_count: u32,
//         shards: u32,
//         hierarchy: bool,
//         sql: HashMap<String, String>,
//         h: String,
//         dist: String,
//     ) -> Self {
//         Self {
//             db_prefix: db_prefix.clone(),
//             table_prefix: table_prefix.clone(),
//             table_postfix: table_postfix.clone(),
//             db_count: db_count,
//             table_count: table_count,
//             hierarchy: hierarchy,
//             sql: sql.clone(),
//             distribution: DBRange::new(db_count as usize, table_count as usize, shards as usize),
//             hasher: Hasher::from(h.as_str()),
//         }
//     }

//     pub fn try_from(item: &MysqlNamespace) -> Self {
//         Self {
//             db_prefix: item.basic.db_prefix.clone(),
//             table_prefix: item.basic.table_prefix.clone(),
//             table_postfix: item.basic.table_postfix.clone(),
//             db_count: item.basic.db_count,
//             table_count: item.basic.table_count,
//             hierarchy: item.basic.hierarchy,
//             sql: item.sql.clone(),
//             distribution: DBRange::new(
//                 item.basic.db_count as usize,
//                 item.basic.table_count as usize,
//                 item.archive.get(ARCHIVE_SHARDS_KEY).len() as usize,
//             ),
//             hasher: Hasher::from(item.basic.hash.as_str()),
//         }
//     }
//     pub fn distribution(&self) -> &DBRange {
//         &self.distribution
//     }
//     pub fn hasher(&self) -> &Hasher {
//         &self.hasher
//     }

//     pub fn get_year(&self, key: &RingSlice) -> String {
//         let id = to_i64(key);
//         let milliseconds = UuidHelper::get_time(id) * 1000;
//         chrono::Utc
//             .timestamp_millis(milliseconds)
//             .with_timezone(&Shanghai)
//             .format("%Y")
//             .to_string()
//     }

//     //todo: sql_name 枚举
//     pub fn build_sql(
//         &self,
//         sql_name: &str,
//         db_key: &RingSlice,
//         tbl_key: &RingSlice,
//     ) -> Option<String> {
//         let tname = match self.build_tname(tbl_key) {
//             Some(tname) => tname,
//             None => return None,
//         };
//         let dname = match self.build_dname(db_key) {
//             Some(dname) => dname,
//             None => return None,
//         };
//         let mut sql = self.sql.get(sql_name).unwrap_or(&String::new()).clone();
//         if !sql.is_empty() {
//             sql = sql.replace(DB_NAME_EXPRESSION, &dname);
//             sql = sql.replace(TABLE_NAME_EXPRESSION, &tname);

//             // TODO 先走通，再优化 fishermen
//             sql = replace_one(&sql, KEY_EXPRESSION, db_key).expect("malformed sql");
//             // sql = sql.replace(KEY_EXPRESSION, did.to_string().as_str());
//         } else {
//             log::error!("find the sql by name {} is empty or null", sql_name);
//         }
//         log::debug!("{}", sql);
//         Some(sql)
//     }

//     // pub fn build_sql_key(db_name: &str, table_name: &str, sql_name: &str) -> String {
//     //     format!("{}.{}.{}", db_name, table_name, sql_name)
//     // }
// }
pub fn build_date_tname(tbl_prefix: &str, key: &RingSlice, is_display_day: bool) -> Option<String> {
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
pub fn replace_one(raw_sql: &String, from: &'static str, to: &RingSlice) -> Result<String> {
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
        None => Err(Error::ResponseProtocolInvalid),
    }
}

pub fn to_i64(key: &RingSlice) -> i64 {
    let mut id = 0_i64;
    const ZERO: u8 = '0' as u8;
    for i in 0..key.len() {
        let c = key.at(i);
        assert!(c.is_ascii_digit(), "malformed key:{:?}", key);
        // id = id * 10 + (c - ZERO) as i64;
        id = id
            .wrapping_mul(10_i64)
            .wrapping_add(c.wrapping_sub(ZERO) as i64);
    }
    id
}
