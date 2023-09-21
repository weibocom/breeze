use super::config::ARCHIVE_DEFAULT_YEAR;
use super::{
    strategy::{to_i64, Postfix},
    uuid::Uuid,
};
use chrono::Datelike;
use core::fmt::Write;
use ds::RingSlice;
use protocol::kv::Strategy;
use sharding::hash::Hash;
use sharding::{distribution::DBRange, hash::Hasher};

#[derive(Default, Clone, Debug)]
pub struct KVTime {
    db_prefix: String,
    table_prefix: String,
    table_postfix: Postfix,
    hasher: Hasher,
    distribution: DBRange,
    years: Vec<u16>,
}

impl KVTime {
    pub fn new(name: String, db_count: u32, shards: u32, years: Vec<u16>) -> Self {
        Self {
            db_prefix: name.clone(),
            table_prefix: name.clone(),
            table_postfix: Postfix::YYMMDD,
            distribution: DBRange::new(db_count as usize, 1usize, shards as usize),
            hasher: Hasher::from("crc32"),
            years: years,
        }
    }
    fn write_date_tname(&self, buf: &mut impl Write, uuid: i64, is_display_day: bool) {
        let s = uuid.date_time();
        let (year, month, day) = (s.year() % 100, s.month(), s.day());
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

    fn write_dname(&self, buf: &mut impl Write, key: &RingSlice) {
        let db_idx: usize = self.distribution.db_idx(self.hasher.hash(key));
        let _ = write!(buf, "{}_{}", self.db_prefix, db_idx);
    }

    #[inline]
    fn default_year(&self) -> u16 {
        // todo 配置里的default到year的映射
        ARCHIVE_DEFAULT_YEAR
    }
    // fn build_idx_tname(&self, key: &RingSlice) -> Option<String> {
    //     let table_prefix = self.table_prefix.clone();
    //     if self.table_count > 0 && self.db_count > 0 {
    //         let mut tbl_index = 0;
    //         tbl_index = self.distribution.table_idx(self.hasher.hash(key));
    //         return Some(format!("{}_{}", table_prefix, tbl_index));
    //     } else {
    //         log::error!("id is null");
    //     }
    //     None
    // }
}
impl Strategy for KVTime {
    fn distribution(&self) -> &DBRange {
        &self.distribution
    }
    fn hasher(&self) -> &Hasher {
        &self.hasher
    }
    fn year(&self, key: &RingSlice) -> u16 {
        let uuid = to_i64(key);
        let year = uuid.year();
        if self.years.contains(&year) {
            year
        } else {
            self.default_year()
        }
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
