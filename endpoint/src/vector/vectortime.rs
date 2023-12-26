use crate::kv::kvtime::KVTime;

use super::strategy::Postfix;
use chrono::NaiveDate;
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

    pub fn get_date(&self, keys: &[RingSlice], keys_name: &[String]) -> Result<NaiveDate, Error> {
        let mut ymd = (0u16, 0u16, 0u16);
        for (i, key_name) in keys_name.iter().enumerate() {
            match key_name.as_str() {
                "yymm" => {
                    ymd = (
                        keys[i]
                            .try_str_num(0..0 + 2)
                            .ok_or(Error::RequestProtocolInvalid)? as u16
                            + 2000,
                        keys[i]
                            .try_str_num(2..2 + 2)
                            .ok_or(Error::RequestProtocolInvalid)? as u16,
                        1,
                    );
                    break;
                }
                "yymmdd" => {
                    ymd = (
                        keys[i]
                            .try_str_num(0..0 + 2)
                            .ok_or(Error::RequestProtocolInvalid)? as u16
                            + 2000,
                        keys[i]
                            .try_str_num(2..2 + 2)
                            .ok_or(Error::RequestProtocolInvalid)? as u16,
                        keys[i]
                            .try_str_num(4..4 + 2)
                            .ok_or(Error::RequestProtocolInvalid)? as u16,
                    );
                    break;
                }
                // "yyyymm" => {
                //     ymd = (
                //         keys[i].try_str_num(0..0+4)? as u16,
                //         keys[i].try_str_num(4..4+2)? as u16,
                //         1,
                //     )
                // }
                // "yyyymmdd" => {
                //     ymd = (
                //         keys[i].try_str_num(0..0+4)? as u16,
                //         keys[i].try_str_num(4..4+2)? as u16,
                //         keys[i].try_str_num(6..6+2)? as u16,
                //     )
                // }
                &_ => {
                    continue;
                }
            }
        }
        NaiveDate::from_ymd_opt(ymd.0.into(), ymd.1.into(), ymd.2.into())
            .ok_or(Error::RequestProtocolInvalid)
    }
    pub fn write_database_table(&self, buf: &mut impl Write, keys: &[RingSlice], hash: i64) {
        self.kvtime.write_dname_with_hash(buf, hash);
        let _ = buf.write_char('.');
        //外部已经判断过日期有效
        let date = self.get_date(keys, &self.keys_name).unwrap();
        self.kvtime.write_tname_with_date(buf, &date)
    }

    pub(crate) fn keys(&self) -> &[String] {
        &self.keys_name
    }

    pub(crate) fn condition_keys(&self) -> impl Iterator<Item = Option<&String>> {
        self.keys_name
            .iter()
            .map(|key_name| match key_name.as_str() {
                "yymm" | "yymmdd" => None,
                // "yyyymm" | "yyyymmdd" => None,
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
