use std::fmt::Write;

pub use crate::kv::strategy::{to_i64, Postfix};
use ds::RingSlice;
use protocol::kv::common::Command;
use protocol::kv::{MysqlBinary, Strategy, VectorSqlBuilder};
use sharding::distribution::DBRange;
use sharding::hash::Hasher;

use super::config::VectorNamespace;
use super::vectortime::VectorTime;

#[derive(Debug, Clone)]
pub enum Strategist {
    VectorTime(VectorTime),
}

impl Default for Strategist {
    #[inline]
    fn default() -> Self {
        Self::VectorTime(VectorTime::new_with_db(
            "status".to_string(),
            "status".to_string(),
            32u32,
            8u32,
            Postfix::YYMMDD,
        ))
    }
}

impl Strategist {
    pub fn try_from(ns: &VectorNamespace) -> Self {
        Self::VectorTime(VectorTime::new_with_db(
            ns.basic.db_name.clone(),
            ns.basic.table_name.clone(),
            ns.basic.db_count,
            //此策略默认所有年都有同样的shard，basic也只配置了一项，也暗示了这个默认
            ns.backends.iter().next().unwrap().1.len() as u32,
            ns.basic.table_postfix.as_str().into(),
        ))
    }
}

impl Strategy for Strategist {
    #[inline]
    fn distribution(&self) -> &DBRange {
        match self {
            Strategist::VectorTime(inner) => Strategy::distribution(inner),
        }
    }
    #[inline]
    fn hasher(&self) -> &Hasher {
        match self {
            Strategist::VectorTime(inner) => Strategy::hasher(inner),
        }
    }
    #[inline]
    fn get_key_for_vector(&self, keys: &[RingSlice]) -> Result<u16, protocol::Error> {
        match self {
            Strategist::VectorTime(inner) => Strategy::get_key_for_vector(inner, keys),
        }
    }
    #[inline]
    fn tablename_len(&self) -> usize {
        match self {
            Strategist::VectorTime(inner) => Strategy::tablename_len(inner),
        }
    }
    #[inline]
    fn write_database_table(&self, buf: &mut impl Write, key: &RingSlice) {
        match self {
            Strategist::VectorTime(inner) => Strategy::write_database_table(inner, buf, key),
        }
    }
}

pub(crate) struct VectorBuilder {}

impl MysqlBinary for VectorBuilder {
    fn mysql_cmd(&self) -> Command {
        todo!()
    }
}

impl VectorSqlBuilder for VectorBuilder {
    fn len(&self) -> usize {
        todo!()
    }

    fn write_sql(&self, packet: &mut protocol::kv::PacketCodec) {
        todo!()
    }
}
