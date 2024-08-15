use chrono::{Datelike, NaiveDate};
use core::fmt::Write;
use protocol::vector::{CommandType, Postfix};
use sharding::{distribution::DBRange, hash::Hasher};

#[derive(Clone, Debug)]
pub struct Batch {
    db_prefix: String,
    table_prefix: String,
    table_postfix: Postfix,
    hasher: Hasher,
    distribution: DBRange,
    keys_name: Vec<String>,
    si_cols: Vec<String>,
    si: Si,
}

impl Batch {
    pub fn new_with_db(
        db_prefix: String,
        table_prefix: String,
        db_count: u32,
        shards: u32,
        table_postfix: Postfix,
        keys_name: Vec<String>,
        si_cols: Vec<String>,
        si_db_prefix: String,
        si_db_count: u32,
        si_table_prefix: String,
        si_table_count: u32,
        si_shards: u32,
    ) -> Self {
        Self {
            db_prefix,
            table_prefix,
            table_postfix,
            distribution: DBRange::new(db_count as usize, 1usize, shards as usize),
            hasher: Hasher::from("crc32"),
            keys_name,
            si_cols,
            si: Si::new(
                si_db_prefix,
                si_db_count,
                si_table_prefix,
                si_table_count,
                si_shards,
            ),
        }
    }

    pub fn distribution(&self) -> &DBRange {
        &self.distribution
    }

    pub fn si_distribution(&self) -> &DBRange {
        self.si.distribution()
    }

    pub fn hasher(&self) -> &Hasher {
        &self.hasher
    }

    pub fn write_dname_with_hash(&self, buf: &mut impl Write, hash: i64) {
        let db_idx: usize = self.distribution.db_idx(hash);
        let _ = write!(buf, "{}_{}", self.db_prefix, db_idx);
    }

    pub fn write_tname_with_date(&self, buf: &mut impl Write, date: &NaiveDate) {
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

    pub fn write_database_table(&self, buf: &mut impl Write, date: &NaiveDate, hash: i64) {
        self.write_dname_with_hash(buf, hash);
        let _ = buf.write_char('.');
        self.write_tname_with_date(buf, date)
    }

    pub(crate) fn write_si_database_table(&self, buf: &mut impl Write, hash: i64) {
        self.si.write_database_table(buf, hash)
    }

    pub(crate) fn keys(&self) -> &[String] {
        &self.keys_name
    }

    // pub(crate) fn get_next_date(&self, year: u16, month: u8) -> NaiveDate {
    //     if month == 1 {
    //         return NaiveDate::from_ymd_opt((year - 1).into(), 12, 1).unwrap();
    //     } else {
    //         return NaiveDate::from_ymd_opt(year.into(), (month - 1).into(), 1).unwrap();
    //     }
    // }

    pub(crate) fn batch(&self, limit: u64, _: &protocol::vector::VectorCmd) -> u64 {
        limit
    }

    pub(crate) fn si_cols(&self) -> &[String] {
        &self.si_cols
    }

    pub(crate) fn keys_len(&self, cmd: CommandType) -> usize {
        match cmd {
            CommandType::VRange => self.keys().len() - 1,
            //相比vrange多了一个日期key
            CommandType::VAdd | CommandType::VDel => self.keys().len(),
            _ => panic!("not sup {cmd:?}"),
        }
    }
}

#[derive(Clone, Debug)]
struct Si {
    db_prefix: String,
    table_prefix: String,
    distribution: DBRange,
}

impl Si {
    fn new(
        db_prefix: String,
        db_count: u32,
        table_prefix: String,
        table_count: u32,
        shards: u32,
    ) -> Self {
        Self {
            db_prefix: db_prefix,
            table_prefix: table_prefix,
            distribution: DBRange::new(db_count as usize, table_count as usize, shards as usize),
        }
    }
    fn distribution(&self) -> &DBRange {
        &self.distribution
    }
    fn write_database_table(&self, buf: &mut impl Write, hash: i64) {
        let db_idx = self.distribution.db_idx(hash);
        let table_idx = self.distribution.table_idx(hash);
        let _ = write!(
            buf,
            "{}_{}.{}_{}",
            self.db_prefix, db_idx, self.table_prefix, table_idx
        );
    }
}
