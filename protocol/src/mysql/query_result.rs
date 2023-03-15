// TODO：传入数据进行parse，后续进一步按需改造

use crate::{Error, Result, Stream};

use crate::mysql::constants::StatusFlags;
pub use crate::mysql::proto::{Binary, Text};

use crate::mysql::rsppacket::ResponsePacket;
use crate::mysql::{io::ParseBuf, packets::OkPacket, row::RowDeserializer, value::ServerSide};

use std::{borrow::Cow, marker::PhantomData, sync::Arc};

use crate::mysql::packets::Column;
use crate::mysql::row::Row;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Or<A, B> {
    A(A),
    B(B),
}

/// Result set kind.
pub(super) trait Protocol: 'static + Send + Sync {
    fn next<'a, S: Stream>(
        rsp_packet: &'a mut ResponsePacket<'a, S>,
        columns: Arc<[Column]>,
    ) -> Result<Option<Row>>;
}

impl Protocol for Text {
    fn next<'a, S: Stream>(
        rsp_packet: &'a mut ResponsePacket<'a, S>,
        columns: Arc<[Column]>,
    ) -> Result<Option<Row>> {
        match rsp_packet.next_row_packet()? {
            Some(pld) => {
                let row = ParseBuf(&*pld).parse::<RowDeserializer<(), Text>>(columns)?;
                Ok(Some(row.into()))
            }
            None => Ok(None),
        }
    }
}

impl Protocol for Binary {
    fn next<'a, S: Stream>(
        rsp_packet: &'a mut ResponsePacket<'a, S>,
        columns: Arc<[Column]>,
    ) -> Result<Option<Row>> {
        match rsp_packet.next_row_packet()? {
            Some(pld) => {
                let row = ParseBuf(&*pld).parse::<RowDeserializer<ServerSide, Binary>>(columns)?;
                Ok(Some(row.into()))
            }
            None => Ok(None),
        }
    }
}

/// State of a result set iterator.
#[derive(Debug)]
enum SetIteratorState {
    /// Iterator is in a non-empty set.
    InSet(Arc<[Column]>),
    /// Iterator is in an empty set.
    InEmptySet(OkPacket<'static>),
    /// Iterator is in an errored result set.
    Errored(Error),
    /// Next result set isn't handled.
    OnBoundary,
    /// No more result sets.
    Done,
}

impl SetIteratorState {
    fn ok_packet(&self) -> Option<&OkPacket<'_>> {
        if let Self::InEmptySet(ref ok) = self {
            Some(ok)
        } else {
            None
        }
    }

    fn columns(&self) -> Option<&Arc<[Column]>> {
        if let Self::InSet(ref cols) = self {
            Some(cols)
        } else {
            None
        }
    }
}

impl From<Vec<Column>> for SetIteratorState {
    fn from(columns: Vec<Column>) -> Self {
        Self::InSet(columns.into())
    }
}

impl From<OkPacket<'static>> for SetIteratorState {
    fn from(ok_packet: OkPacket<'static>) -> Self {
        Self::InEmptySet(ok_packet)
    }
}

impl From<Error> for SetIteratorState {
    fn from(err: Error) -> Self {
        Self::Errored(err)
    }
}

impl From<Or<Vec<Column>, OkPacket<'static>>> for SetIteratorState {
    fn from(or: Or<Vec<Column>, OkPacket<'static>>) -> Self {
        match or {
            Or::A(cols) => Self::from(cols),
            Or::B(ok) => Self::from(ok),
        }
    }
}

/// Response to a query or statement execution.
///
/// It is an iterator:
/// *   over result sets (via `Self::current_set`)
/// *   over rows of a current result set (via `Iterator` impl)
#[derive(Debug)]
pub(super) struct QueryResult<'c, T: crate::mysql::prelude::Protocol, S: Stream> {
    // conn: ConnMut<'c, 't, 'tc>,
    rsp_packet: &'c mut ResponsePacket<'c, S>,
    state: SetIteratorState,
    set_index: usize,
    // status_flags: StatusFlags,
    protocol: PhantomData<T>,
}

impl<'c, T: crate::mysql::prelude::Protocol, S: Stream> QueryResult<'c, T, S> {
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

    pub(super) fn new(
        rsp_packet: &'c mut ResponsePacket<'c, S>,
        meta: Or<Vec<Column>, OkPacket<'static>>,
    ) -> QueryResult<'c, T, S> {
        Self::from_state(rsp_packet, meta.into())
    }

    /// Updates state with the next result set, if any.
    ///
    /// Returns `false` if there is no next result set.
    ///
    /// **Requires:** `self.state == OnBoundary`
    /// TODO 这个方法搬到rsppacket中去实现 fishermen
    pub(super) fn handle_next(&mut self) {
        debug_assert!(
            matches!(self.state, SetIteratorState::OnBoundary),
            "self.state != OnBoundary"
        );

        // if status_flags.contains(StatusFlags::SERVER_MORE_RESULTS_EXISTS) {}

        if self.rsp_packet.more_results_exists() {
            match self.rsp_packet.handle_result_set() {
                Ok(meta) => self.state = meta.into(),
                Err(err) => self.state = err.into(),
            }
            self.set_index += 1;
        } else {
            self.state = SetIteratorState::Done;
        }
    }

    /// Returns an iterator over the current result set.
    #[deprecated = "Please use QueryResult::iter"]
    pub fn next_set<'d>(&'d mut self) -> Option<ResultSet<'c, 'd, T, S>> {
        self.iter()
    }

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

    /// Returns the number of affected rows for the current result set.
    pub fn affected_rows(&self) -> u64 {
        self.state
            .ok_packet()
            .map(|ok| ok.affected_rows())
            .unwrap_or_default()
    }

    /// Returns the last insert id for the current result set.
    pub fn last_insert_id(&self) -> Option<u64> {
        self.state
            .ok_packet()
            .map(|ok| ok.last_insert_id())
            .unwrap_or_default()
    }

    /// Returns the warnings count for the current result set.
    pub fn warnings(&self) -> u16 {
        self.state
            .ok_packet()
            .map(|ok| ok.warnings())
            .unwrap_or_default()
    }

    /// [Info] for the current result set.
    ///
    /// Will be empty if not defined.
    ///
    /// [Info]: http://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
    pub fn info_ref(&self) -> &[u8] {
        self.state
            .ok_packet()
            .and_then(|ok| ok.info_ref())
            .unwrap_or_default()
    }

    /// [Info] for the current result set.
    ///
    /// Will be empty if not defined.
    ///
    /// [Info]: http://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
    pub fn info_str(&self) -> Cow<str> {
        self.state
            .ok_packet()
            .and_then(|ok| ok.info_str())
            .unwrap_or_else(|| "".into())
    }

    /// Returns columns of the current result rest.
    pub fn columns(&self) -> SetColumns {
        SetColumns {
            inner: self.state.columns().map(Into::into),
        }
    }
}

impl<'c, T: crate::mysql::prelude::Protocol, S: Stream> Drop for QueryResult<'c, T, S> {
    fn drop(&mut self) {
        while self.iter().is_some() {}
    }
}

#[derive(Debug)]
pub(super) struct ResultSet<'a, 'd, T: crate::mysql::prelude::Protocol, S: Stream> {
    set_index: usize,
    inner: &'d mut QueryResult<'a, T, S>,
}

impl<'a, 'b, 'c, T: crate::mysql::prelude::Protocol, S: Stream> std::ops::Deref
    for ResultSet<'a, '_, T, S>
{
    type Target = QueryResult<'a, T, S>;

    fn deref(&self) -> &Self::Target {
        &*self.inner
    }
}

impl<T: crate::mysql::prelude::Protocol, S: Stream> Iterator for ResultSet<'_, '_, T, S> {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.set_index == self.inner.set_index {
            self.inner.next()
        } else {
            None
        }
    }
}

impl<'c, T: crate::mysql::prelude::Protocol, S: Stream> Iterator for QueryResult<'c, T, S> {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        use SetIteratorState::*;

        let state = std::mem::replace(&mut self.state, OnBoundary);
        match state {
            // InSet(cols) => match T::next(&mut self.rsp_packet, cols.clone()) {
            //     Ok(Some(row)) => {
            //         self.state = InSet(cols.clone());
            //         Some(Ok(row))
            //     }
            //     Ok(None) => {
            //         self.handle_next();
            //         None
            //     }
            //     Err(e) => {
            //         self.handle_next();
            //         Some(Err(e))
            //     }
            // },
            InSet(cols) => match self.rsp_packet.next_row_packet() {
                Ok(Some(pld)) => {
                    log::debug!("+++ read row data: {:?}", pld);
                    match ParseBuf(&*pld).parse::<RowDeserializer<(), Text>>(cols.clone()) {
                        Ok(row) => {
                            log::debug!("+++ parsed row: {:?}", row);
                            self.state = InSet(cols.clone());
                            Some(Ok(row.into()))
                        }
                        Err(e) => {
                            log::warn!("+++ parsed row failed: {:?}, data: {:?}", e, pld);
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

impl<T: crate::mysql::prelude::Protocol, S: Stream> Drop for ResultSet<'_, '_, T, S> {
    fn drop(&mut self) {
        while self.next().is_some() {}
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SetColumns<'a> {
    inner: Option<&'a Arc<[Column]>>,
}

impl<'a> SetColumns<'a> {
    /// Returns an index of a column by its name.
    pub fn column_index<U: AsRef<str>>(&self, name: U) -> Option<usize> {
        let name = name.as_ref().as_bytes();
        self.inner
            .as_ref()
            .and_then(|cols| cols.iter().position(|col| col.name_ref() == name))
    }

    pub fn as_ref(&self) -> &[Column] {
        self.inner
            .as_ref()
            .map(|cols| &(*cols)[..])
            .unwrap_or(&[][..])
    }
}
