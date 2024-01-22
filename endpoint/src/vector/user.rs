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
    db_postfix: String,
    table_postfix: String,
    shards: u32,
    hasher: Hasher,
    dist: DBRange,
    keys_name: Vec<String>,
}

impl User {
    pub fn new(
        db_prefix: String,
        table_prefix: String,
        db_postfix: String,
        table_postfix: String,
        db_count: u32,
        table_count: u32,
        shards: u32,
        keys_name: Vec<String>,
    ) -> Self {
        Self {
            db_prefix,
            table_prefix,
            db_postfix,
            table_postfix,
            dist: DBRange::new_user(db_count as usize, table_count as usize),
            hasher: Hasher::from("crc32"),
            shards,
            keys_name,
        }
    }

    pub fn distribution(&self) -> &DBRange {
        &self.dist
    }

    pub fn hasher(&self) -> &Hasher {
        &self.hasher
    }

    pub fn write_database_table(&self, buf: &mut impl Write, hash: i64) {
        let Self {
            db_prefix,
            table_prefix,
            db_postfix,
            table_postfix,
            dist,
            ..
        } = self;
        if db_postfix.is_empty() {
            let _ = buf.write_str(db_prefix);
        } else {
            let db_idx: usize = dist.db_idx(hash);
            let _ = write!(buf, "{}_{}", db_prefix, db_idx);
        }
        let _ = buf.write_char('.');
        if table_postfix.is_empty() {
            let _ = buf.write_str(table_prefix);
        } else {
            let table_idx: usize = dist.table_idx(hash);
            let _ = write!(buf, "{}_{}", table_prefix, table_idx);
        }
    }

    pub(crate) fn keys(&self) -> &[String] {
        &self.keys_name
    }

    pub(crate) fn condition_keys(&self) -> Box<dyn Iterator<Item = Option<&String>> + '_> {
        Box::new(self.keys_name.iter().map(|k| Some(k)))
    }
}
