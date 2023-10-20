use crate::kv::kvtime::KVTime;

use super::strategy::Postfix;
use core::fmt::Write;
use ds::RingSlice;
use protocol::kv::Strategy;
use sharding::{distribution::DBRange, hash::Hasher};

#[derive(Clone, Debug)]
pub struct VectorTime {
    kvtime: KVTime,
}

impl Strategy for VectorTime {
    fn distribution(&self) -> &DBRange {
        <KVTime as Strategy>::distribution(&self.kvtime)
    }

    fn hasher(&self) -> &Hasher {
        <KVTime as Strategy>::hasher(&self.kvtime)
    }

    fn get_key(&self, key: &RingSlice) -> u16 {
        <KVTime as Strategy>::get_key(&self.kvtime, key)
    }

    fn tablename_len(&self) -> usize {
        <KVTime as Strategy>::tablename_len(&self.kvtime)
    }

    fn write_database_table(&self, buf: &mut impl Write, key: &RingSlice) {
        <KVTime as Strategy>::write_database_table(&self.kvtime, buf, key)
    }
}

impl std::ops::Deref for VectorTime {
    type Target = KVTime;

    fn deref(&self) -> &Self::Target {
        &self.kvtime
    }
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
}
