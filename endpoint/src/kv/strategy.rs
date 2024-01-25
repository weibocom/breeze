use std::fmt::Write;

use super::config::KvNamespace;
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

impl Into<Postfix> for &str {
    fn into(self) -> Postfix {
        match self.to_lowercase().as_str() {
            "yymm" => Postfix::YYMM,
            _ => Postfix::YYMMDD,
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
            Postfix::YYMMDD,
        ))
    }
}

impl Strategist {
    pub fn try_from(ns: &KvNamespace) -> Self {
        Self::KVTime(KVTime::new(
            ns.basic.db_name.clone(),
            ns.basic.db_count,
            //此策略默认所有年都有同样的shard，basic也只配置了一项，也暗示了这个默认
            ns.backends.iter().next().unwrap().1.len() as u32,
            ns.basic.table_postfix.as_str().into(),
        ))
    }
}
