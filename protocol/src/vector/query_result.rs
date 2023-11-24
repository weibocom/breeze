use crate::kv::error::Result;
use crate::{Command, Stream};

pub use crate::kv::common::proto::{Binary, Text};

use super::rsppacket::ResponsePacket;
use crate::kv::common::{io::ParseBuf, packets::OkPacket, row::RowDeserializer};

use std::{marker::PhantomData, sync::Arc};

use crate::kv::common::packets::Column;
use crate::kv::common::query_result::{Or, SetIteratorState};
use crate::kv::common::row::Row;
use SetIteratorState::*;
/// Response to a query or statement execution.
///
/// It is an iterator:
/// *   over result sets (via `Self::current_set`)
/// *   over rows of a current result set (via `Iterator` impl)
#[derive(Debug)]
pub(crate) struct QueryResult<'c, T: crate::kv::prelude::Protocol, S: Stream> {
    // conn: ConnMut<'c, 't, 'tc>,
    rsp_packet: &'c mut ResponsePacket<'c, S>,
    state: SetIteratorState,
    set_index: usize,
    // status_flags: StatusFlags,
    protocol: PhantomData<T>,
}

impl<'c, T: crate::kv::prelude::Protocol, S: Stream> QueryResult<'c, T, S> {
    fn from_state(
        rsp_packet: &'c mut ResponsePacket<'c, S>,
        state: SetIteratorState,
        // status_flags: StatusFlags,
    ) -> QueryResult<'c, T, S> {
        QueryResult {
            rsp_packet,
            state,
            set_index: 0,
            // status_flags,
            protocol: PhantomData,
        }
    }

    pub fn new(
        rsp_packet: &'c mut ResponsePacket<'c, S>,
        meta: Or<Vec<Column>, OkPacket>,
    ) -> QueryResult<'c, T, S> {
        Self::from_state(rsp_packet, meta.into())
    }

    /// Updates state with the next result set, if any.
    ///
    /// Returns `false` if there is no next result set.
    ///
    /// **Requires:** `self.state == OnBoundary`
    /// TODO 这个方法搬到rsppacket中去实现 fishermen
    pub fn handle_next(&mut self) {
        debug_assert!(
            matches!(self.state, SetIteratorState::OnBoundary),
            "self.state != OnBoundary"
        );

        // if status_flags.contains(StatusFlags::SERVER_MORE_RESULTS_EXISTS) {}
        log::debug!("+++ has more rs:{}", self.rsp_packet.more_results_exists());
        if self.rsp_packet.more_results_exists() {
            match self.rsp_packet.parse_result_set_meta() {
                Ok(meta) => {
                    if self.set_index == 0 {
                        self.state = meta.into();
                    }
                }
                Err(err) => self.state = err.into(),
            }
            self.set_index += 1;
        } else {
            self.state = SetIteratorState::Done;
        }
    }

    // /// Returns an iterator over the current result set.
    // #[deprecated = "Please use QueryResult::iter"]
    // pub fn next_set<'d>(&'d mut self) -> Option<ResultSet<'c, 'd, T, S>> {
    //     self.iter()
    // }

    /// Returns an iterator over the current result set.
    ///
    /// The returned iterator will be consumed either by the caller
    /// or implicitly by the `ResultSet::drop`. This operation
    /// will advance `self` to the next result set (if any).
    ///
    /// The following code describes the behavior:
    ///
    /// ```rust
    /// # mysql::doctest_wrapper!(__result, {
    /// # use mysql::*;
    /// # use mysql::prelude::*;
    /// # let pool = Pool::new(get_opts())?;
    /// # let mut conn = pool.get_conn()?;
    /// # conn.query_drop("CREATE TEMPORARY TABLE mysql.tbl(id INT NOT NULL PRIMARY KEY)")?;
    ///
    /// let mut query_result = conn.query_iter("\
    ///     INSERT INTO mysql.tbl (id) VALUES (3, 4);\
    ///     SELECT * FROM mysql.tbl;
    ///     UPDATE mysql.tbl SET id = id + 1;")?;
    ///
    /// // query_result is on the first result set at the moment
    /// {
    ///     assert_eq!(query_result.affected_rows(), 2);
    ///     assert_eq!(query_result.last_insert_id(), Some(4));
    ///
    ///     let first_result_set = query_result.iter().unwrap();
    ///     assert_eq!(first_result_set.affected_rows(), 2);
    ///     assert_eq!(first_result_set.last_insert_id(), Some(4));
    /// }
    ///
    /// // the first result set is now dropped, so query_result is on the second result set
    /// {
    ///     assert_eq!(query_result.affected_rows(), 0);
    ///     assert_eq!(query_result.last_insert_id(), None);
    ///     
    ///     let mut second_result_set = query_result.iter().unwrap();
    ///
    ///     let first_row = second_result_set.next().unwrap().unwrap();
    ///     assert_eq!(from_row::<u8>(first_row), 3_u8);
    ///     let second_row = second_result_set.next().unwrap().unwrap();
    ///     assert_eq!(from_row::<u8>(second_row), 4_u8);
    ///
    ///     assert!(second_result_set.next().is_none());
    ///
    ///     // second_result_set is consumed but still represents the second result set
    ///     assert_eq!(second_result_set.affected_rows(), 0);
    /// }
    ///
    /// // the second result set is now dropped, so query_result is on the third result set
    /// assert_eq!(query_result.affected_rows(), 2);
    ///
    /// // QueryResult::drop simply does the following:
    /// while query_result.iter().is_some() {}
    /// # });
    /// ```
    pub fn iter<'d>(&'d mut self) -> Option<ResultSet<'c, 'd, T, S>> {
        use SetIteratorState::*;

        if let OnBoundary | Done = &self.state {
            debug_assert!(
                !self.rsp_packet.more_results_exists(),
                "the next state must be handled by the Iterator::next"
            );

            None
        } else {
            Some(ResultSet {
                set_index: self.set_index,
                inner: self,
            })
        }
    }

    /// TODO 代理rsp_packet的同名方法，这两个文件需要进行整合
    #[inline(always)]
    pub fn build_final_rsp_cmd(&mut self, ok: bool, rsp_data: Vec<u8>) -> Command {
        self.rsp_packet.build_final_rsp_cmd(ok, rsp_data)
    }

    // /// 解析meta后面的rows，有可能是完整响应，也有可能返回Err(不完整协议)
    // #[inline(always)]
    // pub fn parse_rows(&mut self) -> Result<Command> {
    //     // rows 收集器
    //     let collector = |mut acc: Vec<Vec<u8>>, row| {
    //         acc.push(from_row(row));
    //         acc
    //     };

    //     // 构建最终返回给client的响应内容，格式详见issues/721
    //     // *2\r\n 固定2
    //     // *2\r\n 列数量
    //     // +like_id\r\n
    //     // +object_id\r\n
    //     // *4\r\n  数据按行展开
    //     // +111\r\n
    //     // +112\r\n
    //     // +121\r\n
    //     // +122\r\n
    //     let mut v: Vec<u8> = Vec::with_capacity(1024);

    //     // 解析row并构建cmd
    //     // self.state状态在下面scan_rows函数内可能会被从InSet修改为其他值，暂存列信息
    //     let columns = match &self.state {
    //         InSet(c) => Some(c.clone()),
    //         _ => None,
    //     };
    //     let result_set = self.scan_rows(Vec::with_capacity(4), collector)?;
    //     let status = result_set.len() > 0;
    //     if status {
    //         // columns有值才会走到这个分支
    //         let cols = columns.unwrap();
    //         // 填充header，field部分
    //         v.extend_from_slice("*2\r\n*".as_bytes());
    //         v.extend_from_slice(&(cols.len().to_le_bytes()));
    //         v.extend_from_slice("\r\n".as_bytes());
    //         for (_, col) in cols.iter().enumerate() {
    //             v.push(b'+');
    //             v.extend_from_slice(&col.name_ref());
    //             v.extend_from_slice("\r\n".as_bytes());
    //         }

    //         // 填充value部分
    //         v.push(b'*');
    //         v.extend_from_slice(&(result_set.len().to_le_bytes()));
    //         v.extend_from_slice("\r\n".as_bytes());
    //         let _ = result_set.iter().map(|r| {
    //             v.push(b'+');
    //             v.extend(r);
    //             v.extend_from_slice("\r\n".as_bytes());
    //         });
    //         return Ok(self.build_final_rsp_cmd(status, v));
    //     }
    //     // else: not found
    //     // use crate::redis::command::PADDING_RSP_TABLE;PADDING_RSP_TABLE[6]
    //     let empty = b"$-1\r\n".to_vec(); // nil
    //     let cmd = self.build_final_rsp_cmd(true, empty);
    //     Ok(cmd)
    // }

    /// 解析meta后面的rows
    #[inline(always)]
    pub(crate) fn parse_rows_to_cmd(&mut self) -> Result<Command> {
        // 解析出mysql rows
        let rows = self.parse_rows()?;

        // 将rows转为redis协议
        let packet_data = format_to_redis(&rows);

        // 构建响应
        Ok(self.build_final_rsp_cmd(true, packet_data))
    }

    pub(crate) fn parse_rows(&mut self) -> Result<Vec<Row>> {
        // 改为每次只处理本次的响应
        let mut rows = Vec::with_capacity(8);
        loop {
            match self.scan_one_row()? {
                Some(row) => {
                    // init = f(init, from_row::<R>(row));
                    rows.push(row);
                }

                None => {
                    // take统一放在构建最终响应的地方进行
                    // let _ = self.rsp_packet.take();
                    return Ok(rows);
                }
            }
        }
    }

    // pub(crate) fn scan_rows<R, F, U>(&mut self, mut init: U, mut f: F) -> Result<U>
    // where
    //     R: FromRow,
    //     F: FnMut(U, R) -> U,
    // {
    //     // 改为每次只处理本次的响应
    //     loop {
    //         match self.scan_one_row()? {
    //             Some(row) => {
    //                 init = f(init, from_row::<R>(row));
    //             }

    //             None => {
    //                 // take统一放在构建最终响应的地方进行
    //                 // let _ = self.rsp_packet.take();
    //                 return Ok(init);
    //             }
    //         }
    //     }
    // }

    fn scan_one_row(&mut self) -> Result<Option<Row>> {
        let state = std::mem::replace(&mut self.state, OnBoundary);
        match state {
            InSet(cols) => match self.rsp_packet.next_row_packet()? {
                Some(pld) => {
                    let row_data =
                        ParseBuf::new(0, *pld).parse::<RowDeserializer<(), Text>>(cols.clone())?;
                    self.state = InSet(cols.clone());
                    return Ok(Some(row_data.into()));
                }
                None => {
                    self.handle_next();
                    return Ok(None);
                }
            },
            InEmptySet(_) => {
                self.handle_next();
                Ok(None)
            }
            Errored(err) => {
                self.handle_next();
                Err(err)
            }
            OnBoundary => {
                self.handle_next();
                Ok(None)
            }
            Done => {
                self.state = Done;
                Ok(None)
            }
        }
    }

    // /// Returns the number of affected rows for the current result set.
    // pub fn affected_rows(&self) -> u64 {
    //     self.state
    //         .ok_packet()
    //         .map(|ok| ok.affected_rows())
    //         .unwrap_or_default()
    // }

    // /// Returns the last insert id for the current result set.
    // pub fn last_insert_id(&self) -> Option<u64> {
    //     self.state
    //         .ok_packet()
    //         .map(|ok| ok.last_insert_id())
    //         .unwrap_or_default()
    // }

    // /// Returns the warnings count for the current result set.
    // pub fn warnings(&self) -> u16 {
    //     self.state
    //         .ok_packet()
    //         .map(|ok| ok.warnings())
    //         .unwrap_or_default()
    // }

    // /// [Info] for the current result set.
    // ///
    // /// Will be empty if not defined.
    // ///
    // /// [Info]: http://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
    // pub fn info_ref(&self) -> &[u8] {
    //     self.state
    //         .ok_packet()
    //         .and_then(|ok| ok.info_ref())
    //         .unwrap_or_default()
    // }

    // /// [Info] for the current result set.
    // ///
    // /// Will be empty if not defined.
    // ///
    // /// [Info]: http://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
    // pub fn info_str(&self) -> Cow<str> {
    //     self.state
    //         .ok_packet()
    //         .and_then(|ok| ok.info_str())
    //         .unwrap_or_else(|| "".into())
    // }

    // /// Returns columns of the current result rest.
    // pub fn columns(&self) -> SetColumns {
    //     SetColumns {
    //         inner: self.state.columns().map(Into::into),
    //     }
    // }
}

impl<'c, T: crate::kv::prelude::Protocol, S: Stream> Drop for QueryResult<'c, T, S> {
    fn drop(&mut self) {
        while self.iter().is_some() {}
    }
}

#[derive(Debug)]
pub(crate) struct ResultSet<'a, 'd, T: crate::kv::prelude::Protocol, S: Stream> {
    set_index: usize,
    inner: &'d mut QueryResult<'a, T, S>,
}

impl<'a, 'b, 'c, T: crate::kv::prelude::Protocol, S: Stream> std::ops::Deref
    for ResultSet<'a, '_, T, S>
{
    type Target = QueryResult<'a, T, S>;

    fn deref(&self) -> &Self::Target {
        &*self.inner
    }
}

impl<T: crate::kv::prelude::Protocol, S: Stream> Iterator for ResultSet<'_, '_, T, S> {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.set_index == self.inner.set_index {
            self.inner.next()
        } else {
            None
        }
    }
}

impl<'c, T: crate::kv::prelude::Protocol, S: Stream> Iterator for QueryResult<'c, T, S> {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        use SetIteratorState::*;

        let state = std::mem::replace(&mut self.state, OnBoundary);
        match state {
            InSet(cols) => match self.rsp_packet.next_row_packet() {
                Ok(Some(pld)) => {
                    log::debug!("+++ read row data: {:?}", pld);
                    match ParseBuf::from(*pld).parse::<RowDeserializer<(), Text>>(cols.clone()) {
                        Ok(row) => {
                            log::debug!("+++ parsed row: {:?}", row);
                            self.state = InSet(cols.clone());
                            Some(Ok(row.into()))
                        }
                        Err(_e) => {
                            log::warn!("+++ parsed row failed: {:?}, data: {:?}", _e, pld);
                            None
                        }
                    }
                }
                Ok(None) => {
                    self.handle_next();
                    None
                }
                Err(e) => {
                    self.handle_next();
                    Some(Err(e))
                }
            },
            InEmptySet(_) => {
                self.handle_next();
                None
            }
            Errored(err) => {
                self.handle_next();
                Some(Err(err))
            }
            OnBoundary => None,
            Done => {
                self.state = Done;
                None
            }
        }
    }
}

impl<T: crate::kv::prelude::Protocol, S: Stream> Drop for ResultSet<'_, '_, T, S> {
    fn drop(&mut self) {
        while self.next().is_some() {}
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SetColumns<'a> {
    inner: Option<&'a Arc<[Column]>>,
}

// impl<'a> SetColumns<'a> {
//     /// Returns an index of a column by its name.
//     pub fn column_index<U: AsRef<str>>(&self, name: U) -> Option<usize> {
//         let name = name.as_ref().as_bytes();
//         self.inner
//             .as_ref()
//             .and_then(|cols| cols.iter().position(|col| col.name_ref() == name))
//     }

//     pub fn as_ref(&self) -> &[Column] {
//         self.inner
//             .as_ref()
//             .map(|cols| &(*cols)[..])
//             .unwrap_or(&[][..])
//     }
// }
/// 先打通，后续再考虑优化fishermen
#[inline]
pub fn format_to_redis(rows: &Vec<Row>) -> Vec<u8> {
    let mut data = Vec::with_capacity(32 * rows.len());
    // 响应为空，返回
    if rows.len() == 0 {
        data.extend_from_slice("$-1\r\n".as_bytes());
        return data;
    }

    // 构建 resp协议的header总array计数 以及 column的计数
    let columns = rows.get(0).expect("columns unexists").columns();
    let prefix = format!("*2\r\n*{}\r\n", columns.len());
    data.extend_from_slice(prefix.as_bytes());

    // 构建columns
    for idx in 0..columns.len() {
        let col = columns.get(idx).expect("column");
        data.push(b'+');
        data.extend_from_slice(col.name_str().as_bytes());
        data.extend_from_slice("\r\n".as_bytes());
    }

    // TODO 构建column values，存在copy，先打通再优化 fishermen
    let val_header = format!("*{}\r\n", columns.len() * rows.len());
    data.extend_from_slice(val_header.as_bytes());
    for ri in 0..rows.len() {
        let row = rows.get(ri).expect("row unexists");
        row.write_as_redis(&mut data);
    }

    data
}
