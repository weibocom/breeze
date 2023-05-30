use super::config::{MysqlNamespace, ARCHIVE_DEFAULT_KEY};
use super::kvtime::KVTime;
use ds::RingSlice;

use enum_dispatch::enum_dispatch;
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

#[enum_dispatch]
pub trait Strategy {
    fn distribution(&self) -> &DBRange;
    fn hasher(&self) -> &Hasher;
    fn get_key(&self, key: &RingSlice) -> Option<String>;
    fn build_kvsql(&self, req: &RingSlice, key: &RingSlice) -> Option<String>;
}

#[enum_dispatch(Strategy)]
#[derive(Debug, Clone)]
pub enum Strategist {
    KVTime(KVTime),
}

impl Default for Strategist {
    #[inline]
    fn default() -> Self {
        Self::KVTime(KVTime::new(
            "status".to_string(),
            32u32,
            8u32,
            vec![ARCHIVE_DEFAULT_KEY.to_string()],
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
            item.backends.keys().cloned().collect(),
        ))
    }

    pub fn new(db_name: String, db_count: u32, shards: u32, years: Vec<String>) -> Self {
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
