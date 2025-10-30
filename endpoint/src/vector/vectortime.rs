use crate::kv::kvtime::KVTime;
use crate::kv::uuid::*;
use chrono::NaiveDate;
use core::fmt::Write;
use ds::RingSlice;
use protocol::vector::Postfix;
use protocol::{kv::Strategy, vector::KeysType};
use protocol::{Error, DATE_YYMM, DATE_YYMMDD};
use sharding::{distribution::DBRange, hash::Hasher};

#[derive(Clone, Debug)]
pub struct VectorTime {
    kvtime: KVTime,
    keys_name: Vec<String>,
}

impl VectorTime {
    pub fn new_with_db(
        db_prefix: String,
        table_prefix: String,
        db_count: u32,
        shards: u32,
        table_postfix: Postfix,
        keys_name: Vec<String>,
    ) -> Self {
        Self {
            kvtime: KVTime::new_with_db(db_prefix, table_prefix, db_count, shards, table_postfix),
            keys_name: keys_name,
        }
    }

    pub fn distribution(&self) -> &DBRange {
        <KVTime as Strategy>::distribution(&self.kvtime)
    }

    pub fn hasher(&self) -> &Hasher {
        <KVTime as Strategy>::hasher(&self.kvtime)
    }

    //策略处已作校验
    pub fn get_date(&self, keys: &[RingSlice]) -> Result<NaiveDate, Error> {
        let date = keys.last().unwrap();
        // keys_name长度为1，则key本身是uuid
        if self.keys_name.len() == 1 {
            let k = &keys[0];
            let uuid = k.uuid();
            return Ok(uuid.native_date())
        }
        let ymd = match self.keys_name.last().unwrap().as_str() {
            DATE_YYMM => (
                date.try_str_num(0..0 + 2)
                    .ok_or(Error::RequestProtocolInvalid)? as u16
                    + 2000,
                date.try_str_num(2..2 + 2)
                    .ok_or(Error::RequestProtocolInvalid)? as u16,
                1,
            ),
            DATE_YYMMDD => (
                date.try_str_num(0..0 + 2)
                    .ok_or(Error::RequestProtocolInvalid)? as u16
                    + 2000,
                date.try_str_num(2..2 + 2)
                    .ok_or(Error::RequestProtocolInvalid)? as u16,
                date.try_str_num(4..4 + 2)
                    .ok_or(Error::RequestProtocolInvalid)? as u16,
            ),
            _ => (0, 0, 0),
            // "yyyymm" => {
            //     ymd = (
            //         date.try_str_num(0..0+4)? as u16,
            //         date.try_str_num(4..4+2)? as u16,
            //         1,
            //     )
            // }
            // "yyyymmdd" => {
            //     ymd = (
            //         date.try_str_num(0..0+4)? as u16,
            //         date.try_str_num(4..4+2)? as u16,
            //         date.try_str_num(6..6+2)? as u16,
            //     )
            // }
        };
        NaiveDate::from_ymd_opt(ymd.0.into(), ymd.1.into(), ymd.2.into())
            .ok_or(Error::RequestProtocolInvalid)
    }
    pub fn write_database_table(&self, buf: &mut impl Write, date: &NaiveDate, hash: i64) {
        self.kvtime.write_dname_with_hash(buf, hash);
        let _ = buf.write_char('.');
        self.kvtime.write_tname_with_date(buf, date)
    }

    pub(crate) fn keys(&self) -> &[String] {
        &self.keys_name
    }

    pub(crate) fn keys_with_type(&self) -> Box<dyn Iterator<Item = KeysType> + '_> {
        Box::new(
            self.keys_name
                .iter()
                .map(|key_name| match key_name.as_str() {
                    DATE_YYMM | DATE_YYMMDD => KeysType::Time,
                    &_ => KeysType::Keys(key_name),
                }),
        )
    }

    pub(crate) fn check_vector_cmd(&self, vcmd: &protocol::vector::VectorCmd) -> Result<(), Error> {
        if vcmd.keys.len() < self.keys_name.len() {
            return Err(Error::RequestProtocolInvalid);
        }
        Ok(())
    }
}

impl std::ops::Deref for VectorTime {
    type Target = KVTime;

    fn deref(&self) -> &Self::Target {
        &self.kvtime
    }
}
