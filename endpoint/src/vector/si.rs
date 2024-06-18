use crate::kv::uuid::*;
use chrono::{Datelike, NaiveDate};
use chrono_tz::Tz;
use core::fmt::Write;
use ds::RingSlice;
use protocol::kv::Strategy;
use protocol::Error;
use sharding::hash::Hash;
use sharding::{distribution::DBRange, hash::Hasher};

#[derive(Clone, Debug)]
pub struct Si {
    db_prefix: String,
    table_prefix: String,
    hasher: Hasher,
    distribution: DBRange,
}

impl Si {
    pub fn new(
        db_prefix: String,
        db_count: u32,
        table_prefix: String,
        table_count: u32,
        shards: u32,
    ) -> Self {
        Self {
            db_prefix: db_prefix,
            table_prefix: table_prefix,
            distribution: DBRange::new(db_count as usize, table_count as usize, shards as usize),
            hasher: Hasher::from("crc32"),
        }
    }
    pub fn distribution(&self) -> &DBRange {
        &self.distribution
    }

    pub fn hasher(&self) -> &Hasher {
        &self.hasher
    }
    fn write_tname(&self, buf: &mut impl Write, key: &RingSlice) {
        let table_idx = self.distribution.table_idx(self.hasher.hash(key));
        let _ = write!(buf, "{}_{}", self.table_prefix, table_idx);
    }

    pub fn write_dname(&self, buf: &mut impl Write, key: &RingSlice) {
        let db_idx: usize = self.distribution.db_idx(self.hasher.hash(key));
        let _ = write!(buf, "{}_{}", self.db_prefix, db_idx);
    }

    pub fn write_dname_with_hash(&self, buf: &mut impl Write, hash: i64) {
        let db_idx: usize = self.distribution.db_idx(hash);
        let _ = write!(buf, "{}_{}", self.db_prefix, db_idx);
    }
}
impl Strategy for Si {
    fn distribution(&self) -> &DBRange {
        &self.distribution
    }
    fn hasher(&self) -> &Hasher {
        &self.hasher
    }
    fn get_key(&self, key: &RingSlice) -> u16 {
        let uuid = key.uuid();
        uuid.year()
    }
    fn tablename_len(&self) -> usize {
        // status_6.status_030926, 11代表除去前缀后的长度
        self.db_prefix.len() + self.table_prefix.len() + 6
    }
    fn write_database_table(&self, buf: &mut impl Write, key: &RingSlice) {
        self.write_dname(buf, key);
        //这里是按照utf8编码写入的
        let _ = buf.write_char('.');
        self.write_tname(buf, key);
    }
}
