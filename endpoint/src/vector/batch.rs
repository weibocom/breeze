use chrono::{Datelike, NaiveDate};
use core::fmt::Write;
use ds::RingSlice;
use protocol::{
    vector::{CommandType, KeysType, Postfix},
    Error, DATE_YYMM, DATE_YYMMDD,
};
use sharding::{distribution::DBRange, hash::Hasher};

#[derive(Clone, Debug)]
pub struct Aggregation {
    db_prefix: String,
    table_prefix: String,
    table_postfix: Postfix,
    hasher: Hasher,
    distribution: DBRange,
    keys_name: Vec<String>,
    si_cols: Vec<String>,
    si: Si,
}

impl Aggregation {
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

    pub fn get_date(&self, cmd: CommandType, keys: &[RingSlice]) -> Result<NaiveDate, Error> {
        match cmd {
            CommandType::VRange | CommandType::VGet => Ok(NaiveDate::default()),
            //相比vrange多了一个日期key
            _ => {
                let date = keys.last().unwrap();
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
                };
                NaiveDate::from_ymd_opt(ymd.0.into(), ymd.1.into(), ymd.2.into())
                    .ok_or(Error::RequestProtocolInvalid)
            }
        }
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

    //校验动态信息，后续有重复校验的后面延迟校验
    //上行、xx.timeline请求多带一个日期key，日期key固定在最后
    pub(crate) fn check_vector_cmd(
        &self,
        vcmd: &protocol::vector::VectorCmd,
    ) -> protocol::Result<()> {
        match vcmd.cmd {
            CommandType::VRange
            | CommandType::VGet
            | CommandType::VAddSi
            | CommandType::VDelSi
            | CommandType::VCard => {
                if vcmd.keys.len() != self.keys().len() - 1 {
                    return Err(Error::RequestProtocolInvalid);
                }
                Ok(())
            }
            //相比vrange多了一个日期key
            CommandType::VAdd
            | CommandType::VDel
            | CommandType::VUpdate
            | CommandType::VAddTimeline
            | CommandType::VDelTimeline
            | CommandType::VRangeTimeline
            | CommandType::VUpdateTimeline => {
                if vcmd.keys.len() != self.keys().len() {
                    return Err(Error::RequestProtocolInvalid);
                }
                Ok(())
            }
            CommandType::Unknown => panic!("not sup {:?}", vcmd.cmd),
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
