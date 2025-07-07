use crate::kv::common::buffer_pool::Buffer;
use crate::kv::common::constants::{CapabilityFlags, MAX_PAYLOAD_LEN};
use crate::kv::error::Result;

pub use crate::kv::common::proto::Text;

use super::packet::MysqlRawPacket;
use crate::kv::common::{io::ParseBuf, packets::OkPacket, row::RowDeserializer};

use std::marker::PhantomData;

use crate::kv::common::packets::{Column, OldEofPacket, ResultSetTerminator};
use crate::kv::common::query_result::{Or, SetIteratorState};
use crate::kv::common::row::Row;
use bytes::BufMut;
use ds::Utf8;
use SetIteratorState::*;

const CRLF: &[u8] = b"\r\n";

/// Response to a query or statement execution.
#[derive(Debug)]
pub(crate) struct QueryResult<T: crate::kv::prelude::Protocol> {
    // conn: ConnMut<'c, 't, 'tc>,
    // rsp_packet: &'c mut ResponsePacket<'c, S>,
    data: MysqlRawPacket,
    has_results: bool,
    state: SetIteratorState,
    // set_index: usize,
    protocol: PhantomData<T>,
}

// impl<'c, T: crate::kv::prelude::Protocol, S: Stream> QueryResult<'c, T, S> {
impl<T: crate::kv::prelude::Protocol> QueryResult<T> {
    fn from_state(
        // rsp_packet: &'c mut ResponsePacket<'c, S>,
        data: MysqlRawPacket,
        has_results: bool,
        state: SetIteratorState,
    ) -> QueryResult<T> {
        QueryResult {
            // rsp_packet,
            data,
            has_results,
            state,
            // set_index: 0,
            protocol: PhantomData,
        }
    }

    pub fn new(
        // rsp_packet: &'c mut ResponsePacket<'c, S>,
        data: MysqlRawPacket,
        has_results: bool,
        meta: Or<Vec<Column>, OkPacket>,
    ) -> QueryResult<T> {
        Self::from_state(data, has_results, meta.into())
    }

    /// Updates state with the next result set, if any.
    ///
    /// Returns `false` if there is no next result set.
    ///
    /// **Requires:** `self.state == OnBoundary`
    /// 不支持存储过程，所以 more_results_exists 始终为false
    pub fn handle_next(&mut self) {
        assert!(
            matches!(self.state, SetIteratorState::OnBoundary),
            "self.state != OnBoundary"
        );

        // 不支持multi-resultset，该响应由存储过程产生
        assert!(
            !self.data.more_results_exists(),
            "unsupport stored programs"
        );
        self.state = SetIteratorState::Done;

        // if status_flags.contains(StatusFlags::SERVER_MORE_RESULTS_EXISTS) {}
        // if self.data.more_results_exists() {
        //     match self.rsp_packet.parse_result_set_meta() {
        //         Ok(meta) => {
        //             if self.set_index == 0 {
        //                 self.state = meta.into();
        //             }
        //         }
        //         Err(err) => self.state = err.into(),
        //     }
        //     self.set_index += 1;
        // } else {
        //     self.state = SetIteratorState::Done;
        // }
    }

    pub(super) fn next_row_packet(&mut self, oft: &mut usize) -> Result<Option<Buffer>> {
        if !self.has_results {
            return Ok(None);
        }

        // TODO 这里会parse出最新的seq，但目前是comm query模式，不会用到，暂时丢弃 fishermen
        let packet = self.data.next_packet(oft)?;
        let pld = packet.payload;
        log::info!("+++ kv packet:{:?}", pld);
        if self
            .data
            .has_capability(CapabilityFlags::CLIENT_DEPRECATE_EOF)
        {
            if pld[0] == 0xfe && pld.len() < MAX_PAYLOAD_LEN {
                self.has_results = false;
                self.data.handle_ok::<ResultSetTerminator>(pld)?;
                return Ok(None);
            }
        } else {
            // 根据mysql doc，EOF_Packet的长度是小于9，先把这里从8改为9，注意观察影响 fishermen
            if pld[0] == 0xfe && pld.len() < 9 {
                self.has_results = false;
                self.data.handle_ok::<OldEofPacket>(pld)?;
                return Ok(None);
            }
        }

        let buff = Buffer::new(pld);
        Ok(Some(buff))
    }

    /// 解析meta后面的rows
    #[inline(always)]
    pub(crate) fn parse_rows_to_redis(&mut self, oft: &mut usize) -> Result<Vec<u8>> {
        // 解析出mysql rows
        // 改为每次只处理本次的响应
        let mut rows = Vec::with_capacity(8);
        loop {
            match self.scan_one_row(oft)? {
                Some(row) => {
                    // init = f(init, from_row::<R>(row));
                    rows.push(row);
                }

                None => {
                    break;
                }
            }
        }

        // 将rows转为redis协议
        Ok(format_to_redis(&rows))
    }

    fn scan_one_row(&mut self, oft: &mut usize) -> Result<Option<Row>> {
        let state = std::mem::replace(&mut self.state, OnBoundary);
        match state {
            InSet(cols) => match self.next_row_packet(oft)? {
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
            InEmptySet(_a) => {
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
}

#[inline]
pub fn format_to_redis(rows: &Vec<Row>) -> Vec<u8> {
    let mut data = Vec::with_capacity(32 * rows.len());
    // 响应为空，返回
    if rows.len() == 0 {
        data.extend_from_slice("$-1\r\n".as_bytes());
        return data;
    }

    let columns = rows.get(0).expect("columns unexists").columns_ref();

    // TODO 后面需要支持 select count(uid) from db.tbl
    // 对于vcard（即select count(*)），可能有1个row，也可能有多个rows(带group by)，转为integer/integer-array;
    // sql 在mesh构建为小写，所以此处可以确认count(*)为小写，两者必须保持一致 fishermen
    const VCARD_NAME: &[u8] = b"count(*)";
    if columns.len() == 1 && columns[0].name_ref().eq(VCARD_NAME) {
        format_for_vcard(rows, &mut data);
        return data;
    }

    // 至此，只有vrange了(select * ..)，后续可能还有其他协议
    format_for_commons(rows, &mut data, columns);
    data
}

fn format_for_vcard(rows: &Vec<Row>, data: &mut Vec<u8>) {
    // vcard特殊响应，转为为redis integer/integer array
    if rows.len() > 1 {
        data.put("*".as_bytes());
        data.put(rows.len().to_string().as_bytes());
        data.put(CRLF);
    }
    rows.get(0).expect("rows empty").write_as_redis(data);
    log::debug!("+++ vcard rs:{}", data.utf8());
}

/// 为vrange等带column header + rows的指令构建响应
fn format_for_commons(rows: &Vec<Row>, data: &mut Vec<u8>, columns: &[Column]) {
    // 构建 resp协议的header总array计数 以及 column的计数
    data.put(&b"*2\r\n*"[..]);
    data.put(columns.len().to_string().as_bytes());
    data.put(CRLF);

    // 构建columns 内容
    for idx in 0..columns.len() {
        let col = columns.get(idx).expect("column");
        data.push(b'+');
        data.put(col.name_str().as_bytes());
        data.put(CRLF);
    }

    // 构建column values
    // 先构建value的header
    let val_count = columns.len() * rows.len();
    data.put_u8(b'*');
    data.put(val_count.to_string().as_bytes());
    data.put(CRLF);
    // 再写入每个row的val
    for ri in 0..rows.len() {
        let row = rows.get(ri).expect("row unexists");
        row.write_as_redis(data);
    }
}
