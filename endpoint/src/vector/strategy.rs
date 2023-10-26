use std::fmt::{Display, Write};

pub use crate::kv::strategy::{to_i64, Postfix};
use chrono::{Date, TimeZone};
use chrono_tz::{Asia::Shanghai, Tz};
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
            &[],
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
            &ns.basic.keys,
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
    pub fn get_date(
        &self,
        keys: &[RingSlice],
        keys_name: &[String],
    ) -> Result<(u16, u16, u16), protocol::Error> {
        match self {
            Strategist::VectorTime(inner) => inner.get_date(keys, keys_name),
        }
    }
    fn write_database_table(&self, buf: &mut impl Write, keys: &[RingSlice]) {
        match self {
            Strategist::VectorTime(inner) => inner.write_database_table(buf, keys),
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

struct VectorRingSlice<'a>(&'a RingSlice);
impl<'a> Display for VectorRingSlice<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (s1, s2) = self.0.data();
        f.write_str(unsafe { std::str::from_utf8_unchecked(s1) })?;
        f.write_str(unsafe { std::str::from_utf8_unchecked(s2) })?;
        Ok(())
    }
}

struct Table<'a>(&'a Strategist, &'a [RingSlice]);
impl<'a> Display for Table<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.write_database_table(f, self.1);
        Ok(())
    }
}

impl<'a> VectorSqlBuilder for VectorBuilder<'a> {
    fn len(&self) -> usize {
        todo!();
    }

    fn write_sql(&self, buf: &mut impl Write) {
        match self.op {
            vector::OP_VRANGE => {
                let _ = write!(
                    buf,
                    "select {} from {} where ",
                    VectorRingSlice(&self.vcmd.fields),
                    Table(&self.strategy, &self.vcmd.keys)
                );
            }
            //校验应该在parser_req出
            _ => panic!("not support op:{}", self.op),
        }
    }
}
