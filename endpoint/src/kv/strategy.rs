use std::fmt::Write;

use super::config::{MysqlNamespace, ARCHIVE_DEFAULT_KEY, ARCHIVE_DEFAULT_KEY_U16};
use super::kvtime::KVTime;
use ds::RingSlice;

use protocol::kv::Strategy;
use sharding::distribution::DBRange;
use sharding::hash::Hasher;

#[derive(Debug, Clone)]
pub enum Postfix {
    YYMM,
    YYMMDD,
    INDEX,
}
impl Default for Postfix {
    #[inline]
    fn default() -> Self {
        Self::YYMMDD
    }
}

impl Postfix {
    pub fn value(&self) -> i32 {
        match self {
            Postfix::YYMM => 0,
            Postfix::YYMMDD => 1,
            Postfix::INDEX => 2,
        }
    }
}

#[derive(Debug, Clone)]
pub enum Strategist {
    KVTime(KVTime),
}

impl Strategy for Strategist {
    #[inline]
    fn distribution(&self) -> &DBRange {
        match self {
            Strategist::KVTime(inner) => Strategy::distribution(inner),
        }
    }
    #[inline]
    fn hasher(&self) -> &Hasher {
        match self {
            Strategist::KVTime(inner) => Strategy::hasher(inner),
        }
    }
    #[inline]
    fn get_key(&self, key: &RingSlice) -> u16 {
        match self {
            Strategist::KVTime(inner) => Strategy::get_key(inner, key),
        }
    }
    #[inline]
    fn tablename_len(&self) -> usize {
        match self {
            Strategist::KVTime(inner) => Strategy::tablename_len(inner),
        }
    }
    #[inline]
    fn write_database_table(&self, buf: &mut impl Write, key: &RingSlice) {
        match self {
            Strategist::KVTime(inner) => Strategy::write_database_table(inner, buf, key),
        }
    }
}

impl Default for Strategist {
    #[inline]
    fn default() -> Self {
        Self::KVTime(KVTime::new(
            "status".to_string(),
            32u32,
            8u32,
            vec![ARCHIVE_DEFAULT_KEY_U16],
        ))
    }
}

impl Strategist {
    pub fn try_from(item: &MysqlNamespace) -> Self {
        Self::KVTime(KVTime::new(
            item.basic.db_name.clone(),
            item.basic.db_count,
            item.backends
                .get(ARCHIVE_DEFAULT_KEY)
                .expect("ARCHIVE_DEFAULT_KEY null")
                .len() as u32,
            item.backends
                .keys()
                .map(|x| x.parse::<u16>().unwrap_or(ARCHIVE_DEFAULT_KEY_U16))
                .collect(),
        ))
    }

    pub fn new(db_name: String, db_count: u32, shards: u32, years: Vec<u16>) -> Self {
        Self::KVTime(KVTime::new(db_name, db_count, shards, years))
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
