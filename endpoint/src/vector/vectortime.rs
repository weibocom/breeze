use crate::kv::kvtime::KVTime;

use super::strategy::{to_i64, Postfix};
use chrono::DateTime;
use chrono_tz::Tz;
use core::fmt::Write;
use ds::RingSlice;
use protocol::kv::Strategy;
use protocol::Error;
use sharding::{distribution::DBRange, hash::Hasher};

#[derive(Clone, Debug)]
pub struct VectorTime {
    kvtime: KVTime,
}

impl VectorTime {
    pub fn new_with_db(
        db_prefix: String,
        table_prefix: String,
        db_count: u32,
        shards: u32,
        table_postfix: Postfix,
    ) -> Self {
        Self {
            kvtime: KVTime::new_with_db(db_prefix, table_prefix, db_count, shards, table_postfix),
        }
    }

    pub fn distribution(&self) -> &DBRange {
        <KVTime as Strategy>::distribution(&self.kvtime)
    }

    pub fn hasher(&self) -> &Hasher {
        <KVTime as Strategy>::hasher(&self.kvtime)
    }

    pub fn get_year(&self, keys: &[RingSlice], keys_name: &[String]) -> Result<u16, Error> {
        if keys.len() == keys_name.len() {
            for (i, key_name) in keys_name.iter().enumerate() {
                if key_name == "yymm" {
                    return Ok(to_i64(&keys[i].slice(0, 4)) as u16);
                }
            }
        }
        return Err(Error::ProtocolIncomplete);
    }
    pub fn write_database_table(&self, buf: &mut impl Write, key: &RingSlice, date: DateTime<Tz>) {}
}

impl std::ops::Deref for VectorTime {
    type Target = KVTime;

    fn deref(&self) -> &Self::Target {
        &self.kvtime
    }
}
