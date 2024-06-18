use crate::kv::kvtime::KVTime;

use super::strategy::Postfix;
use chrono::{Datelike, NaiveDate};
use chrono_tz::Tz;
use core::fmt::Write;
use ds::RingSlice;
use protocol::kv::Strategy;
use protocol::Error;
use sharding::{distribution::DBRange, hash::Hasher};

#[derive(Clone, Debug)]
pub struct Batch {
    kvtime: KVTime,
    keys_name: Vec<String>,
    si_cols: Vec<String>,
}

impl Batch {
    pub fn new_with_db(
        db_prefix: String,
        table_prefix: String,
        db_count: u32,
        shards: u32,
        table_postfix: Postfix,
        keys_name: Vec<String>,
        si_cols: Vec<String>,
        si_db_prefix: String,
        si_db_count: u32,
        si_table_prefix: String,
        si_table_count: u32,
    ) -> Self {
        Self {
            kvtime: KVTime::new_with_db(db_prefix, table_prefix, db_count, shards, table_postfix),
            keys_name,
            si_cols,
        }
    }

    pub fn distribution(&self) -> &DBRange {
        <KVTime as Strategy>::distribution(&self.kvtime)
    }

    pub fn hasher(&self) -> &Hasher {
        <KVTime as Strategy>::hasher(&self.kvtime)
    }

    pub fn get_date(&self, _: &[RingSlice]) -> Result<NaiveDate, Error> {
        let now = chrono::Utc::now().with_timezone(&Tz::Asia__Shanghai);
        Ok(NaiveDate::from_ymd_opt(now.year(), now.month(), now.day()).unwrap())
    }
    pub fn write_database_table(&self, buf: &mut impl Write, date: &NaiveDate, hash: i64) {
        // let db_idx: usize = self.distribution.db_idx(hash);
        // let _ = write!(buf, "{}_{}", self.db_prefix, db_idx);
        let _ = buf.write_char('.');
        // self.kvtime.write_tname_with_date(buf, date)
    }

    pub(crate) fn keys(&self) -> &[String] {
        &self.keys_name
    }

    // pub(crate) fn get_next_date(&self, year: u16, month: u8) -> NaiveDate {
    //     if month == 1 {
    //         return NaiveDate::from_ymd_opt((year - 1).into(), 12, 1).unwrap();
    //     } else {
    //         return NaiveDate::from_ymd_opt(year.into(), (month - 1).into(), 1).unwrap();
    //     }
    // }

    pub(crate) fn batch(&self, limit: u64, _: &protocol::vector::VectorCmd) -> u64 {
        limit
    }

    pub(crate) fn si_cols(&self) -> &[String] {
        &self.si_cols
    }
}

impl std::ops::Deref for Batch {
    type Target = KVTime;

    fn deref(&self) -> &Self::Target {
        &self.kvtime
    }
}
