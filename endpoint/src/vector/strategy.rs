use std::fmt::Write;

pub use crate::kv::strategy::{to_i64, Postfix};
use ds::RingSlice;
use protocol::kv::common::Command;
use protocol::kv::{MysqlBinary, Strategy, VectorSqlBuilder};
use protocol::{vector, vector::VectorCmd, OpCode};
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

//vector的Strategy用来确定以下几点：
//3. 如何从keys中计算hash和year
//1. 数据库表名的格式如 table_yymm
//2. 库名表名后缀如何计算
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
    #[inline]
    pub fn distribution(&self) -> &DBRange {
        match self {
            Strategist::VectorTime(inner) => inner.distribution(),
        }
    }
    #[inline]
    pub fn hasher(&self) -> &Hasher {
        match self {
            Strategist::VectorTime(inner) => inner.hasher(),
        }
    }
    #[inline]
    pub fn get_year(
        &self,
        keys: &[RingSlice],
        keys_name: &[String],
    ) -> Result<u16, protocol::Error> {
        match self {
            Strategist::VectorTime(inner) => inner.get_year(keys, keys_name),
        }
    }
}

pub(crate) struct VectorBuilder<'a> {
    op: OpCode,
    vcmd: &'a VectorCmd,
    strategy: &'a Strategist,
}

impl<'a> VectorBuilder<'a> {
    pub fn new(op: OpCode, vcmd: &'a VectorCmd, strategy: &'a Strategist) -> Self {
        Self { op, vcmd, strategy }
    }
}

impl<'a> MysqlBinary for VectorBuilder<'a> {
    fn mysql_cmd(&self) -> Command {
        match self.op {
            vector::OP_VRANGE => Command::COM_QUERY,
            //校验应该在parser_req出
            _ => panic!("not support op:{}", self.op),
        }
    }
}

impl<'a> VectorSqlBuilder for VectorBuilder<'a> {
    fn len(&self) -> usize {
        todo!()
    }

    fn write_sql(&self, buf: &mut impl Write) {
        todo!()
    }
}
