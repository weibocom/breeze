use std::fmt::Write;

pub use crate::kv::strategy::Postfix;
use chrono::NaiveDate;
use ds::RingSlice;
use protocol::Result;
use sharding::distribution::DBRange;
use sharding::hash::Hasher;

use super::config::VectorNamespace;
use super::user::User;
use super::vectortime::VectorTime;

#[derive(Debug, Clone)]
pub enum Strategist {
    VectorTime(VectorTime),
    User(User),
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
        match ns.basic.strategy.as_str() {
            "user" => Self::User(User::new(
                ns.basic.db_name.clone(),
                ns.basic.table_name.clone(),
                ns.basic.table_postfix.clone(),
                ns.basic.table_postfix.clone(),
                ns.basic.db_count,
                ns.basic.table_count,
                ns.backends_all.len() as u32,
                ns.basic.keys.clone(),
            )),
            _ => {
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
        }
    }
    #[inline]
    pub fn distribution(&self) -> &DBRange {
        match self {
            Strategist::VectorTime(inner) => inner.distribution(),
            Strategist::User(inner) => inner.distribution(),
        }
    }
    #[inline]
    pub fn hasher(&self) -> &Hasher {
        match self {
            Strategist::VectorTime(inner) => inner.hasher(),
            Strategist::User(inner) => inner.hasher(),
        }
    }
    #[inline]
    pub fn get_date(&self, keys: &[RingSlice]) -> Result<NaiveDate> {
        match self {
            Strategist::VectorTime(inner) => inner.get_date(keys),
            Strategist::User(_) => panic!("User not impl get_date"),
        }
    }
}

impl protocol::vector::Strategy for Strategist {
    fn keys(&self) -> &[String] {
        match self {
            Strategist::VectorTime(inner) => inner.keys(),
            Strategist::User(inner) => inner.keys(),
        }
    }
    fn condition_keys(&self) -> Box<dyn Iterator<Item = Option<&String>> + '_> {
        match self {
            Strategist::VectorTime(inner) => inner.condition_keys(),
            Strategist::User(inner) => inner.condition_keys(),
        }
    }
    fn write_database_table(&self, buf: &mut impl Write, date: &NaiveDate, hash: i64) {
        match self {
            Strategist::VectorTime(inner) => inner.write_database_table(buf, date, hash),
            Strategist::User(inner) => inner.write_database_table(buf, hash),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use protocol::{
        kv::VectorSqlBuilder,
        vector::{mysql::*, *},
    };
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
                table_count: 0,
                keys: vec!["kid".into(), "yymm".into()],
                strategy: Default::default(),
                password: Default::default(),
                user: Default::default(),
                region_enabled: Default::default(),
            },
            backends_all: Default::default(),
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
            cmd: CommandType::VRange,
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
        let hash = strategy.hasher().hash(&"id".as_bytes());
        let date = NaiveDate::from_ymd_opt(2021, 5, 1).unwrap();
        let builder = SqlBuilder::new(&vector_cmd, hash, date, &strategy).unwrap();
        builder.write_sql(buf);
        println!("len: {}, act len: {}", builder.len(), buf.len());
        let db_idx = strategy.distribution().db_idx(hash);
        assert_eq!(
            buf,
            &format!("select a from db_name_{db_idx}.table_name_2105 where `kid`='id'")
        );

        // vrange 无field
        let vector_cmd = VectorCmd {
            cmd: CommandType::VRange,
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
        let hash = strategy.hasher().hash(&"id".as_bytes());
        let date = NaiveDate::from_ymd_opt(2021, 5, 1).unwrap();
        let builder = SqlBuilder::new(&vector_cmd, hash, date, &strategy).unwrap();
        buf.clear();
        builder.write_sql(buf);
        println!("len: {}, act len: {}", builder.len(), buf.len());
        let db_idx = strategy.distribution().db_idx(hash);
        assert_eq!(
            buf,
            &format!("select * from db_name_{db_idx}.table_name_2105 where `kid`='id'")
        );

        // 复杂vrange
        let vector_cmd = VectorCmd {
            cmd: CommandType::VRange,
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
        let hash = strategy.hasher().hash(&"id".as_bytes());
        let date = NaiveDate::from_ymd_opt(2021, 5, 1).unwrap();
        let builder = SqlBuilder::new(&vector_cmd, hash, date, &strategy).unwrap();
        buf.clear();
        builder.write_sql(buf);
        println!("len: {}, act len: {}", builder.len(), buf.len());
        let db_idx = strategy.distribution().db_idx(hash);
        assert_eq!(
            buf,
            &format!("select a,b from db_name_{db_idx}.table_name_2105 where `kid`='id' and `a`='1' and `b` in (2,3) group by b order by a,b desc limit 24 offset 12")
            );

        // vcard
        let vector_cmd = VectorCmd {
            cmd: CommandType::VCard,
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
        let hash = strategy.hasher().hash(&"id".as_bytes());
        let date = NaiveDate::from_ymd_opt(2021, 5, 1).unwrap();
        let builder = SqlBuilder::new(&vector_cmd, hash, date, &strategy).unwrap();
        buf.clear();
        builder.write_sql(buf);
        println!("len: {}, act len: {}", builder.len(), buf.len());
        let db_idx = strategy.distribution().db_idx(hash);
        assert_eq!(
            buf,
            &format!("select count(*) from db_name_{db_idx}.table_name_2105 where `kid`='id' and `a`='1' and `b` in (2,3) order by a desc limit 24 offset 12")
            );

        //vadd
        let vector_cmd = VectorCmd {
            cmd: CommandType::VAdd,
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
        let hash = strategy.hasher().hash(&"id".as_bytes());
        let date = NaiveDate::from_ymd_opt(2021, 5, 1).unwrap();
        let builder = SqlBuilder::new(&vector_cmd, hash, date, &strategy).unwrap();
        buf.clear();
        builder.write_sql(buf);
        println!("len: {}, act len: {}", builder.len(), buf.len());
        let db_idx = strategy.distribution().db_idx(hash);
        assert_eq!(
            buf,
            &format!(
                "insert into db_name_{db_idx}.table_name_2105 (`kid`,`a`,`b`) values ('id','1','bb')"
            )
        );

        //vupdate
        let vector_cmd = VectorCmd {
            cmd: CommandType::VUpdate,
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
        let hash = strategy.hasher().hash(&"id".as_bytes());
        let date = NaiveDate::from_ymd_opt(2021, 5, 1).unwrap();
        let builder = SqlBuilder::new(&vector_cmd, hash, date, &strategy).unwrap();
        buf.clear();
        builder.write_sql(buf);
        println!("len: {}, act len: {}", builder.len(), buf.len());
        let db_idx = strategy.distribution().db_idx(hash);
        assert_eq!(
            buf,
            &format!("update db_name_{db_idx}.table_name_2105 set `a`='1',`b`='bb' where `kid`='id' and `a`='1' and `b` in (2,3)")
        );

        //vdel
        let vector_cmd = VectorCmd {
            cmd: CommandType::VDel,
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
        let hash = strategy.hasher().hash(&"id".as_bytes());
        let date = NaiveDate::from_ymd_opt(2021, 5, 1).unwrap();
        let builder = SqlBuilder::new(&vector_cmd, hash, date, &strategy).unwrap();
        buf.clear();
        builder.write_sql(buf);
        println!("len: {}, act len: {}", builder.len(), buf.len());
        let db_idx = strategy.distribution().db_idx(hash);
        assert_eq!(
                buf,
                &format!("delete from db_name_{db_idx}.table_name_2105 where `kid`='id' and `a`='1' and `b` in (2,3)")
                );

        // vget
        let vector_cmd = VectorCmd {
            cmd: CommandType::VGet,
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
        let hash = strategy.hasher().hash(&"id".as_bytes());
        let date = NaiveDate::from_ymd_opt(2021, 5, 1).unwrap();
        let builder = SqlBuilder::new(&vector_cmd, hash, date, &strategy).unwrap();
        buf.clear();
        builder.write_sql(buf);
        println!("len: {}, act len: {}", builder.len(), buf.len());
        let db_idx = strategy.distribution().db_idx(hash);
        assert_eq!(
            buf,
            &format!("select a from db_name_{db_idx}.table_name_2105 where `kid`='id'")
        );

        // vget 无field
        let vector_cmd = VectorCmd {
            cmd: CommandType::VGet,
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
        let hash = strategy.hasher().hash(&"id".as_bytes());
        let date = NaiveDate::from_ymd_opt(2021, 5, 1).unwrap();
        let builder = SqlBuilder::new(&vector_cmd, hash, date, &strategy).unwrap();
        buf.clear();
        builder.write_sql(buf);
        println!("len: {}, act len: {}", builder.len(), buf.len());
        let db_idx = strategy.distribution().db_idx(hash);
        assert_eq!(
            buf,
            &format!("select * from db_name_{db_idx}.table_name_2105 where `kid`='id'")
        );

        // 复杂vget
        let vector_cmd = VectorCmd {
            cmd: CommandType::VGet,
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
        let hash = strategy.hasher().hash(&"id".as_bytes());
        let date = NaiveDate::from_ymd_opt(2021, 5, 1).unwrap();
        let builder = SqlBuilder::new(&vector_cmd, hash, date, &strategy).unwrap();
        buf.clear();
        builder.write_sql(buf);
        println!("len: {}, act len: {}", builder.len(), buf.len());
        let db_idx = strategy.distribution().db_idx(hash);
        assert_eq!(
            buf,
            &format!("select a,b from db_name_{db_idx}.table_name_2105 where `kid`='id' and `a`='1' and `b` in (2,3) group by b order by a,b desc limit 24 offset 12")
            );
    }
}
