use crate::kv::{kvtime::KVTime, strategy::to_i64_err};

use super::strategy::{to_i64, Postfix};
use chrono::TimeZone;
use chrono_tz::Asia::Shanghai;
use core::fmt::Write;
use ds::RingSlice;
use protocol::kv::Strategy;
use protocol::Error;
use sharding::{distribution::DBRange, hash::Hasher};

#[derive(Clone, Debug)]
pub struct VectorTime {
    kvtime: KVTime,
    keys_name: Vec<String>,
}

impl VectorTime {
    pub fn new_with_db(
        db_prefix: String,
        table_prefix: String,
        db_count: u32,
        shards: u32,
        table_postfix: Postfix,
        keys_name: Vec<String>,
    ) -> Self {
        Self {
            kvtime: KVTime::new_with_db(db_prefix, table_prefix, db_count, shards, table_postfix),
            keys_name: keys_name,
        }
    }

    pub fn distribution(&self) -> &DBRange {
        <KVTime as Strategy>::distribution(&self.kvtime)
    }

    pub fn hasher(&self) -> &Hasher {
        <KVTime as Strategy>::hasher(&self.kvtime)
    }

    pub fn get_date(
        &self,
        keys: &[RingSlice],
        keys_name: &[String],
    ) -> Result<(u16, u16, u16), Error> {
        // if keys.len() == keys_name.len() {
        for (i, key_name) in keys_name.iter().enumerate() {
            match key_name.as_str() {
                "yyyymm" => {
                    return Ok((
                        to_i64_err(&keys[i].slice(0, 4))? as u16,
                        to_i64_err(&keys[i].slice(4, 2))? as u16,
                        1,
                    ))
                }
                "yyyymmdd" => {
                    return Ok((
                        to_i64_err(&keys[i].slice(0, 4))? as u16,
                        to_i64_err(&keys[i].slice(4, 2))? as u16,
                        to_i64_err(&keys[i].slice(6, 2))? as u16,
                    ))
                }
                &_ => {
                    continue;
                }
            }
        }
        return Err(Error::ProtocolIncomplete);
    }
    pub fn write_database_table(&self, buf: &mut impl Write, keys: &[RingSlice]) {
        self.kvtime.write_dname(buf, &keys[0]);
        let _ = buf.write_char('.');
        let date = self.get_date(keys, &self.keys_name).unwrap();
        self.kvtime.write_tname_with_date(
            buf,
            &Shanghai.ymd(date.0.into(), date.1.into(), date.2.into()),
        )
    }

    pub(crate) fn keys(&self) -> &[String] {
        &self.keys_name
    }

    pub(crate) fn condition_keys(&self) -> impl Iterator<Item = Option<&String>> {
        self.keys_name
            .iter()
            .map(|key_name| match key_name.as_str() {
                "yyyymm" | "yyyymmdd" => None,
                &_ => Some(key_name),
            })
    }
}

impl std::ops::Deref for VectorTime {
    type Target = KVTime;

    fn deref(&self) -> &Self::Target {
        &self.kvtime
    }
}
