// use std::collections::HashMap;

// use crate::Error;

// use super::{mcpacket::Binary, Result};
// use ds::RingSlice;

// use super::reqpacket::{SqlType, TypeConvert};

// const SQL_INSERT: &'static str = "insert into $db$.$tb$ (id, content) values($k$, $v$)";
// const SQL_UPDATE: &'static str = "update $db$.$tb$ set content=$v$ where id=$k$";
// const SQL_DELETE: &'static str = "delete from $db$.$tb$ where id=$k$";
// const SQL_SELECT: &'static str = "select content from $db$.$tb$ where id=$k$";

// #[derive(Clone, Debug)]
// pub(super) struct MysqlStrategy {
//     sqls: HashMap<SqlType, &'static str>,
// }

// impl Default for MysqlStrategy {
//     fn default() -> Self {
//         let mut sqls = HashMap::with_capacity(4);
//         // TODO sqls需要考虑如何传入
//         sqls.insert(SqlType::Insert, SQL_INSERT);
//         sqls.insert(SqlType::Update, SQL_UPDATE);
//         sqls.insert(SqlType::Delete, SQL_DELETE);
//         sqls.insert(SqlType::Select, SQL_SELECT);

//         Self { sqls: sqls }
//     }
// }

// impl MysqlStrategy {
//     // TODO 除了参数替换，把db、table也放着这里实现了？待讨论 fishermen
//     pub(super) fn build_sql(&self, raw_mc_req: &RingSlice) -> Result<String> {
//         let sql_type = raw_mc_req.sql_type();
//         if !self.sqls.contains_key(&sql_type) {
//             return Err(Error::RequestProtocolInvalid("unknown sql type"));
//         }
//         let sql = *self.sqls.get(&sql_type).unwrap();

//         // TODO DB、table 计算

//         // TODO key、value替换，先走通，还有很大的替换空间
//         match sql_type {
//             SqlType::Insert | SqlType::Update => {
//                 let mut sql = replace_key(sql, &raw_mc_req.key())?;
//                 sql = replace_val(sql.as_str(), &raw_mc_req.value())?;
//                 Ok(sql)
//             }
//             SqlType::Delete | SqlType::Select => {
//                 let sql = replace_key(sql, &raw_mc_req.key())?;
//                 Ok(sql)
//             }
//             _ => {
//                 log::warn!("unsupport mysql req now: {:?}", sql_type);
//                 Err(Error::ResponseProtocolInvalid)
//             }
//         }
//     }
// }

// fn replace_key(raw_sql: &'static str, key: &RingSlice) -> Result<String> {
//     const key_pattern: &str = "$k$";
//     replace_one(raw_sql, key_pattern, key)
// }

// fn replace_val(raw_sql: &str, value: &RingSlice) -> Result<String> {
//     const val_pattern: &str = "$v$";
//     replace_one(raw_sql, val_pattern, value)
// }

// fn replace_one(raw_sql: &str, from: &'static str, to: &RingSlice) -> Result<String> {
//     match raw_sql.find(from) {
//         Some(start) => {
//             let end = start + from.len();
//             Ok(format!(
//                 "{} {} {}",
//                 raw_sql.get(0..start).unwrap(),
//                 to,
//                 raw_sql.get(end..).unwrap()
//             ))
//         }
//         None => Err(Error::ResponseProtocolInvalid),
//     }
// }
