use crate::kv::kvtime::KVTime;

use super::strategy::Postfix;
use chrono::NaiveDate;
use core::fmt::Write;
use ds::RingSlice;
use protocol::kv::Strategy;
use protocol::Error;
use sharding::{distribution::DBRange, hash::Hasher};

#[derive(Clone, Debug)]
pub struct User {
    db_prefix: String,
    table_prefix: String,
    shards: u32,
    hash: Hasher,
    dist: DBRange,
    keys_name: Vec<String>,
}

impl User {
    pub fn new(
        db_prefix: String,
        table_prefix: String,
        db_count: u32,
        table_count: u32,
        shards: u32,
        keys_name: Vec<String>,
    ) -> Self {
        Self {
            db_prefix,
            table_prefix,
            dist: DBRange::new_user(db_count as usize, table_count as usize),
            hash: Hasher::from("crc32"),
            shards,
            keys_name,
        }
    }

    pub fn distribution(&self) -> &DBRange {
        &self.dist
    }

    pub fn hasher(&self) -> &Hasher {
        &self.hash
    }

    pub fn write_database_table(&self, buf: &mut impl Write, date: &NaiveDate, hash: i64) {
        // self.kvtime.write_dname_with_hash(buf, hash);
        // let _ = buf.write_char('.');
        // self.kvtime.write_tname_with_date(buf, date)
    }

    pub(crate) fn keys(&self) -> &[String] {
        &self.keys_name
    }

    pub(crate) fn condition_keys(&self) -> Box<dyn Iterator<Item = Option<&String>> + '_> {
        Box::new(self.keys_name.iter().map(|k| Some(k)))
    }
}
