use std::fmt::{Display, Write};

pub use crate::kv::strategy::{to_i64, Postfix};
use ds::RingSlice;
use protocol::kv::common::Command;
use protocol::kv::{MysqlBinary, VectorSqlBuilder};
use protocol::vector::{CommandType, Condition, Field, VectorCmd};
use protocol::{Error, Result};
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
            Vec::new(),
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
            ns.basic.keys.clone(),
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
    pub fn get_date(&self, keys: &[RingSlice], keys_name: &[String]) -> Result<(u16, u16, u16)> {
        match self {
            Strategist::VectorTime(inner) => inner.get_date(keys, keys_name),
        }
    }
    fn keys(&self) -> &[String] {
        match self {
            Strategist::VectorTime(inner) => inner.keys(),
        }
    }
    pub fn condition_keys(&self) -> impl Iterator<Item = Option<&String>> {
        match self {
            Strategist::VectorTime(inner) => inner.condition_keys(),
        }
    }
    fn write_database_table(&self, buf: &mut impl Write, keys: &[RingSlice]) {
        match self {
            Strategist::VectorTime(inner) => inner.write_database_table(buf, keys),
        }
    }
}

struct VRingSlice<'a>(&'a RingSlice);
impl<'a> Display for VRingSlice<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (s1, s2) = self.0.data();
        f.write_str(unsafe { std::str::from_utf8_unchecked(s1) })?;
        f.write_str(unsafe { std::str::from_utf8_unchecked(s2) })?;
        Ok(())
    }
}

struct Keys<'a>(&'a RingSlice);
impl<'a> Display for Keys<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        //todo: 改成新dev的实现，或者提供一个iter
        // let mut start = 0;
        // while let Some(end) = self.0.find(start, b'|') {
        //     if start == 0 {
        //         let _ = write!(f, "{}", VRingSlice(&self.0.slice(start, end - start)));
        //     } else {
        //         let _ = write!(f, ",{}", VRingSlice(&self.0.slice(start, end - start)));
        //     }
        //     start = end + 1;
        // }
        // let left = self.0.len() - start;
        // assert!(left > 0);
        // if start == 0 {
        //     let _ = write!(f, "{}", VRingSlice(&self.0.slice(start, left)));
        // } else {
        //     let _ = write!(f, ",{}", VRingSlice(&self.0.slice(start, left)));
        // }
        VRingSlice(self.0).fmt(f)
    }
}

struct Key<'a>(&'a RingSlice);
impl<'a> Display for Key<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "`{}`", VRingSlice(self.0))
    }
}

struct Val<'a>(&'a RingSlice);
impl<'a> Display for Val<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let _ = f.write_char('\'');
        self.0.visit(|c| protocol::kv::escape_mysql_and_push(f, c));
        let _ = f.write_char('\'');
        Ok(())
    }
}

struct ConditionDisplay<'a>(&'a Condition);
impl<'a> Display for ConditionDisplay<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.op.equal(b"in") {
            let _ = write!(
                f,
                "{} {} ({})",
                Key(&self.0.field),
                VRingSlice(&self.0.op),
                //todo:暂时不转义
                VRingSlice(&self.0.value)
            );
        } else {
            let _ = write!(
                f,
                "{}{}{}",
                Key(&self.0.field),
                VRingSlice(&self.0.op),
                Val(&self.0.value)
            );
        }
        Ok(())
    }
}

struct Select<'a>(Option<&'a Field>);
impl<'a> Display for Select<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(field) => Keys(&field.1).fmt(f),
            None => f.write_char('*'),
        }
    }
}

struct Table<'a>(&'a Strategist, &'a [RingSlice]);
impl<'a> Display for Table<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.write_database_table(f, self.1);
        Ok(())
    }
}

struct InsertCols<'a>(&'a Strategist, &'a Vec<Field>);
impl<'a> Display for InsertCols<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let &Self(strategy, fields) = self;
        for (i, key) in strategy.condition_keys().enumerate() {
            if let Some(key) = key {
                if i == 0 {
                    let _ = write!(f, "`{}`", key);
                } else {
                    let _ = write!(f, ",`{}`", key);
                }
            }
        }
        for field in fields {
            let _ = write!(f, ",{}", Key(&field.0));
        }
        Ok(())
    }
}

struct InsertVals<'a>(&'a Strategist, &'a Vec<RingSlice>, &'a Vec<Field>);
impl<'a> Display for InsertVals<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let &Self(strategy, keys, fields) = self;
        for (i, key) in strategy.condition_keys().enumerate() {
            if let Some(_) = key {
                if i == 0 {
                    let _ = write!(f, "{}", Val(&keys[i]));
                } else {
                    let _ = write!(f, ",{}", Val(&keys[i]));
                }
            }
        }
        for field in fields {
            let _ = write!(f, ",{}", Val(&field.1));
        }
        Ok(())
    }
}

struct UpdateFields<'a>(&'a Vec<Field>);
impl<'a> Display for UpdateFields<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, field) in self.0.iter().enumerate() {
            if i == 0 {
                let _ = write!(f, "{}={}", Key(&field.0), Val(&field.1));
            } else {
                let _ = write!(f, ",{}={}", Key(&field.0), Val(&field.1));
            }
        }
        Ok(())
    }
}

struct KeysAndCondsAndOrderAndLimit<'a>(&'a Strategist, &'a VectorCmd);
impl<'a> Display for KeysAndCondsAndOrderAndLimit<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let &Self(
            strategy,
            VectorCmd {
                keys,
                fields: _,
                wheres,
                order,
                limit,
                group_by,
            },
        ) = self;
        for (i, key) in strategy.condition_keys().enumerate() {
            if let Some(key) = key {
                if i == 0 {
                    let _ = write!(f, "`{}`={}", key, Val(&keys[i]));
                } else {
                    let _ = write!(f, " and `{}`={}", key, Val(&keys[i]));
                }
            }
        }
        for w in wheres {
            let _ = write!(f, " and {}", ConditionDisplay(w));
        }
        if group_by.fields.len() != 0 {
            let _ = write!(f, " group by {}", Keys(&group_by.fields));
        }
        if order.field.len() != 0 {
            let _ = write!(
                f,
                " order by {} {}",
                Keys(&order.field),
                VRingSlice(&order.order)
            );
        }
        if limit.offset.len() != 0 {
            let _ = write!(
                f,
                " limit {} offset {}",
                VRingSlice(&limit.limit),
                VRingSlice(&limit.offset)
            );
        }
        Ok(())
    }
}

pub(crate) struct VectorBuilder<'a> {
    cmd_type: CommandType,
    vcmd: &'a VectorCmd,
    strategy: &'a Strategist,
}

impl<'a> VectorBuilder<'a> {
    pub fn new(
        cmd_type: CommandType,
        vcmd: &'a VectorCmd,
        strategy: &'a Strategist,
    ) -> Result<Self> {
        if vcmd.keys.len() != strategy.keys().len() {
            Err(Error::ProtocolIncomplete)
        } else {
            Ok(Self {
                cmd_type,
                vcmd,
                strategy,
            })
        }
    }
}

impl<'a> MysqlBinary for VectorBuilder<'a> {
    fn mysql_cmd(&self) -> Command {
        Command::COM_QUERY
    }
}

impl<'a> VectorSqlBuilder for VectorBuilder<'a> {
    fn len(&self) -> usize {
        128
    }

    fn write_sql(&self, buf: &mut impl Write) {
        // let cmd_type = vector::get_cmd_type(self.op).unwrap_or(vector::CommandType::Unknown);
        match self.cmd_type {
            CommandType::VRange => {
                let _ = write!(
                    buf,
                    "select {} from {} where {}",
                    Select(self.vcmd.fields.get(0)),
                    Table(&self.strategy, &self.vcmd.keys),
                    KeysAndCondsAndOrderAndLimit(&self.strategy, &self.vcmd),
                );
            }
            CommandType::VCard => {
                let _ = write!(
                    buf,
                    "select count(*) from {} where {}",
                    Table(&self.strategy, &self.vcmd.keys),
                    KeysAndCondsAndOrderAndLimit(&self.strategy, &self.vcmd),
                );
            }
            CommandType::VAdd => {
                let _ = write!(
                    buf,
                    "insert into {} ({}) values ({})",
                    Table(&self.strategy, &self.vcmd.keys),
                    InsertCols(&self.strategy, &self.vcmd.fields),
                    InsertVals(&self.strategy, &self.vcmd.keys, &self.vcmd.fields),
                );
            }
            CommandType::VUpdate => {
                let _ = write!(
                    buf,
                    "update {} set {} where {}",
                    Table(&self.strategy, &self.vcmd.keys),
                    UpdateFields(&self.vcmd.fields),
                    KeysAndCondsAndOrderAndLimit(&self.strategy, &self.vcmd),
                );
            }
            CommandType::VDel => {
                let _ = write!(
                    buf,
                    "delete from {} where {}",
                    Table(&self.strategy, &self.vcmd.keys),
                    KeysAndCondsAndOrderAndLimit(&self.strategy, &self.vcmd),
                );
            }
            _ => {
                //校验应该在parser_req出
                panic!("not support cmd_type:{:?}", self.cmd_type);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use protocol::vector::{GroupBy, Limit, Order};
    use sharding::hash::Hash;

    use crate::{kv::config::Years, vector::config::Basic};
    //   basic:
    //     resource_type: mysql
    //     selector: distance
    //     timeout_ms_master: 10000
    //     timeout_ms_slave: 10000
    //     db_name: db_name
    //     db_count: 32
    //     table_name: table_name
    //     table_postfix: yymm
    //     keys: [id, yymm]
    //     strategy: vector
    //     user: user
    //     password: password
    //   backends:
    //     2005-2099:
    //       - 127.0.0.1:8080,127.0.0.2:8080
    //       - 127.0.0.1:8081,127.0.0.2:8081
    use super::*;
    #[test]
    fn cmd() {
        let ns = VectorNamespace {
            basic: Basic {
                resource_type: Default::default(),
                selector: Default::default(),
                timeout_ms_master: Default::default(),
                timeout_ms_slave: Default::default(),
                db_name: "db_name".into(),
                table_name: "table_name".into(),
                table_postfix: "yymm".into(),
                db_count: 32,
                keys: vec!["kid".into(), "yymm".into()],
                strategy: Default::default(),
                password: Default::default(),
                user: Default::default(),
            },
            backends_flaten: Default::default(),
            backends: HashMap::from([(
                Years(2005, 2099),
                vec![
                    "127.0.0.1:8080,127.0.0.2:8080".into(),
                    "127.0.0.1:8081,127.0.0.2:8081".into(),
                ],
            )]),
        };
        let strategy = Strategist::try_from(&ns);
        let mut buf = String::new();
        let buf = &mut buf;
        // vrange
        let vector_cmd = VectorCmd {
            keys: vec![
                RingSlice::from_slice("id".as_bytes()),
                RingSlice::from_slice("2105".as_bytes()),
            ],
            fields: vec![(
                RingSlice::from_slice("field".as_bytes()),
                RingSlice::from_slice("a".as_bytes()),
            )],
            wheres: Default::default(),
            group_by: Default::default(),
            order: Default::default(),
            limit: Default::default(),
        };
        let builder = VectorBuilder::new(CommandType::VRange, &vector_cmd, &strategy).unwrap();
        builder.write_sql(buf);
        let db_idx = strategy
            .distribution()
            .db_idx(strategy.hasher().hash(&"id".as_bytes()));
        assert_eq!(
            buf,
            &format!("select a from db_name_{db_idx}.table_name_2105 where `kid`='id'")
        );

        // vrange 无field
        let vector_cmd = VectorCmd {
            keys: vec![
                RingSlice::from_slice("id".as_bytes()),
                RingSlice::from_slice("2105".as_bytes()),
            ],
            fields: Default::default(),
            wheres: Default::default(),
            group_by: Default::default(),
            order: Default::default(),
            limit: Default::default(),
        };
        let builder = VectorBuilder::new(CommandType::VRange, &vector_cmd, &strategy).unwrap();
        buf.clear();
        builder.write_sql(buf);
        let db_idx = strategy
            .distribution()
            .db_idx(strategy.hasher().hash(&"id".as_bytes()));
        assert_eq!(
            buf,
            &format!("select * from db_name_{db_idx}.table_name_2105 where `kid`='id'")
        );

        // 复杂vrange
        let vector_cmd = VectorCmd {
            keys: vec![
                RingSlice::from_slice("id".as_bytes()),
                RingSlice::from_slice("2105".as_bytes()),
            ],
            fields: vec![(
                RingSlice::from_slice("field".as_bytes()),
                RingSlice::from_slice("a,b".as_bytes()),
            )],
            wheres: vec![
                Condition {
                    field: RingSlice::from_slice("a".as_bytes()),
                    op: RingSlice::from_slice("=".as_bytes()),
                    value: RingSlice::from_slice("1".as_bytes()),
                },
                Condition {
                    field: RingSlice::from_slice("b".as_bytes()),
                    op: RingSlice::from_slice("in".as_bytes()),
                    value: RingSlice::from_slice("2,3".as_bytes()),
                },
            ],
            group_by: GroupBy {
                fields: RingSlice::from_slice("b".as_bytes()),
            },
            order: Order {
                field: RingSlice::from_slice("a,b".as_bytes()),
                order: RingSlice::from_slice("desc".as_bytes()),
            },
            limit: Limit {
                offset: RingSlice::from_slice("12".as_bytes()),
                limit: RingSlice::from_slice("24".as_bytes()),
            },
        };
        let builder = VectorBuilder::new(CommandType::VRange, &vector_cmd, &strategy).unwrap();
        buf.clear();
        builder.write_sql(buf);
        let db_idx = strategy
            .distribution()
            .db_idx(strategy.hasher().hash(&"id".as_bytes()));
        assert_eq!(
            buf,
            &format!("select a,b from db_name_{db_idx}.table_name_2105 where `kid`='id' and `a`='1' and `b` in (2,3) group by b order by a,b desc limit 24 offset 12")
            );

        // vcard
        let vector_cmd = VectorCmd {
            keys: vec![
                RingSlice::from_slice("id".as_bytes()),
                RingSlice::from_slice("2105".as_bytes()),
            ],
            fields: vec![],
            wheres: vec![
                Condition {
                    field: RingSlice::from_slice("a".as_bytes()),
                    op: RingSlice::from_slice("=".as_bytes()),
                    value: RingSlice::from_slice("1".as_bytes()),
                },
                Condition {
                    field: RingSlice::from_slice("b".as_bytes()),
                    op: RingSlice::from_slice("in".as_bytes()),
                    value: RingSlice::from_slice("2,3".as_bytes()),
                },
            ],
            group_by: Default::default(),
            order: Order {
                field: RingSlice::from_slice("a".as_bytes()),
                order: RingSlice::from_slice("desc".as_bytes()),
            },
            limit: Limit {
                offset: RingSlice::from_slice("12".as_bytes()),
                limit: RingSlice::from_slice("24".as_bytes()),
            },
        };
        let builder = VectorBuilder::new(CommandType::VCard, &vector_cmd, &strategy).unwrap();
        buf.clear();
        builder.write_sql(buf);
        let db_idx = strategy
            .distribution()
            .db_idx(strategy.hasher().hash(&"id".as_bytes()));
        assert_eq!(
            buf,
            &format!("select count(*) from db_name_{db_idx}.table_name_2105 where `kid`='id' and `a`='1' and `b` in (2,3) order by a desc limit 24 offset 12")
            );

        //vadd
        let vector_cmd = VectorCmd {
            keys: vec![
                RingSlice::from_slice("id".as_bytes()),
                RingSlice::from_slice("2105".as_bytes()),
            ],
            fields: vec![
                (
                    RingSlice::from_slice("a".as_bytes()),
                    RingSlice::from_slice("1".as_bytes()),
                ),
                (
                    RingSlice::from_slice("b".as_bytes()),
                    RingSlice::from_slice("bb".as_bytes()),
                ),
            ],
            wheres: vec![],
            group_by: Default::default(),
            order: Order {
                field: RingSlice::empty(),
                order: RingSlice::empty(),
            },
            limit: Limit {
                offset: RingSlice::empty(),
                limit: RingSlice::empty(),
            },
        };
        let builder = VectorBuilder::new(CommandType::VAdd, &vector_cmd, &strategy).unwrap();
        buf.clear();
        builder.write_sql(buf);
        let db_idx = strategy
            .distribution()
            .db_idx(strategy.hasher().hash(&"id".as_bytes()));
        assert_eq!(
            buf,
            &format!(
                "insert into db_name_{db_idx}.table_name_2105 (`kid`,`a`,`b`) values ('id','1','bb')"
            )
        );

        //vupdate
        let vector_cmd = VectorCmd {
            keys: vec![
                RingSlice::from_slice("id".as_bytes()),
                RingSlice::from_slice("2105".as_bytes()),
            ],
            fields: vec![
                (
                    RingSlice::from_slice("a".as_bytes()),
                    RingSlice::from_slice("1".as_bytes()),
                ),
                (
                    RingSlice::from_slice("b".as_bytes()),
                    RingSlice::from_slice("bb".as_bytes()),
                ),
            ],
            wheres: vec![
                Condition {
                    field: RingSlice::from_slice("a".as_bytes()),
                    op: RingSlice::from_slice("=".as_bytes()),
                    value: RingSlice::from_slice("1".as_bytes()),
                },
                Condition {
                    field: RingSlice::from_slice("b".as_bytes()),
                    op: RingSlice::from_slice("in".as_bytes()),
                    value: RingSlice::from_slice("2,3".as_bytes()),
                },
            ],
            group_by: Default::default(),
            order: Order {
                field: RingSlice::empty(),
                order: RingSlice::empty(),
            },
            limit: Limit {
                offset: RingSlice::empty(),
                limit: RingSlice::empty(),
            },
        };
        let builder = VectorBuilder::new(CommandType::VUpdate, &vector_cmd, &strategy).unwrap();
        buf.clear();
        builder.write_sql(buf);
        let db_idx = strategy
            .distribution()
            .db_idx(strategy.hasher().hash(&"id".as_bytes()));
        assert_eq!(
            buf,
            &format!("update db_name_{db_idx}.table_name_2105 set `a`='1',`b`='bb' where `kid`='id' and `a`='1' and `b` in (2,3)")
        );

        //vdel
        let vector_cmd = VectorCmd {
            keys: vec![
                RingSlice::from_slice("id".as_bytes()),
                RingSlice::from_slice("2105".as_bytes()),
            ],
            fields: vec![],
            wheres: vec![
                Condition {
                    field: RingSlice::from_slice("a".as_bytes()),
                    op: RingSlice::from_slice("=".as_bytes()),
                    value: RingSlice::from_slice("1".as_bytes()),
                },
                Condition {
                    field: RingSlice::from_slice("b".as_bytes()),
                    op: RingSlice::from_slice("in".as_bytes()),
                    value: RingSlice::from_slice("2,3".as_bytes()),
                },
            ],
            group_by: Default::default(),
            order: Order {
                field: RingSlice::empty(),
                order: RingSlice::empty(),
            },
            limit: Limit {
                offset: RingSlice::empty(),
                limit: RingSlice::empty(),
            },
        };
        let builder = VectorBuilder::new(CommandType::VDel, &vector_cmd, &strategy).unwrap();
        buf.clear();
        builder.write_sql(buf);
        let db_idx = strategy
            .distribution()
            .db_idx(strategy.hasher().hash(&"id".as_bytes()));
        assert_eq!(
                buf,
                &format!("delete from db_name_{db_idx}.table_name_2105 where `kid`='id' and `a`='1' and `b` in (2,3)")
                );
    }
}
