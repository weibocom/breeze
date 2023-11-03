use super::{
    strategy::{to_i64, Postfix},
    uuid::Uuid,
};
use chrono::{Date, Datelike};
use chrono_tz::Tz;
use core::fmt::Write;
use ds::RingSlice;
use protocol::kv::Strategy;
use sharding::hash::Hash;
use sharding::{distribution::DBRange, hash::Hasher};

#[derive(Clone, Debug)]
pub struct KVTime {
    db_prefix: String,
    table_prefix: String,
    table_postfix: Postfix,
    hasher: Hasher,
    distribution: DBRange,
}

impl KVTime {
    pub fn new_with_db(
        db_prefix: String,
        table_prefix: String,
        db_count: u32,
        shards: u32,
        table_postfix: Postfix,
    ) -> Self {
        Self {
            db_prefix,
            table_prefix,
            table_postfix,
            distribution: DBRange::new(db_count as usize, 1usize, shards as usize),
            hasher: Hasher::from("crc32"),
        }
    }
    pub fn new(name: String, db_count: u32, shards: u32, table_postfix: Postfix) -> Self {
        Self {
            db_prefix: name.clone(),
            table_prefix: name.clone(),
            table_postfix: table_postfix,
            distribution: DBRange::new(db_count as usize, 1usize, shards as usize),
            hasher: Hasher::from("crc32"),
        }
    }
    fn write_date_tname(&self, buf: &mut impl Write, uuid: i64, is_display_day: bool) {
        let (mut year, month, day) = uuid.ymd();
        year %= 100;
        if is_display_day {
            let _ = write!(
                buf,
                "{}_{:02}{:02}{:02}",
                &self.table_prefix, year, month, day
            );
        } else {
            let _ = write!(buf, "{}_{:02}{:02}", &self.table_prefix, year, month);
        }
    }

    fn write_tname(&self, buf: &mut impl Write, key: &RingSlice) {
        let uuid = to_i64(key);
        match self.table_postfix {
            Postfix::YYMM => {
                self.write_date_tname(buf, uuid, false);
            }
            //Postfix::YYMMDD
            _ => {
                self.write_date_tname(buf, uuid, true);
            }
        }
    }

    pub fn write_dname(&self, buf: &mut impl Write, key: &RingSlice) {
        let db_idx: usize = self.distribution.db_idx(self.hasher.hash(key));
        let _ = write!(buf, "{}_{}", self.db_prefix, db_idx);
    }

    pub fn write_tname_with_date(&self, buf: &mut impl Write, date: &Date<Tz>) {
        let (mut year, month, day) = (date.year(), date.month(), date.day());
        year %= 100;
        match self.table_postfix {
            Postfix::YYMM => {
                let _ = write!(buf, "{}_{:02}{:02}", &self.table_prefix, year, month);
            }
            //Postfix::YYMMDD
            _ => {
                let _ = write!(
                    buf,
                    "{}_{:02}{:02}{:02}",
                    &self.table_prefix, year, month, day
                );
            }
        }
    }
}
impl Strategy for KVTime {
    fn distribution(&self) -> &DBRange {
        &self.distribution
    }
    fn hasher(&self) -> &Hasher {
        &self.hasher
    }
    fn get_key(&self, key: &RingSlice) -> u16 {
        let uuid = to_i64(key);
        uuid.year()
    }
    fn tablename_len(&self) -> usize {
        // status_6.status_030926, 11代表除去前缀后的长度
        self.db_prefix.len() + self.table_prefix.len() + 11
    }
    fn write_database_table(&self, buf: &mut impl Write, key: &RingSlice) {
        self.write_dname(buf, key);
        //这里是按照utf8编码写入的
        let _ = buf.write_char('.');
        self.write_tname(buf, key);
    }
}
