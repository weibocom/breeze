use chrono::NaiveDate;
use core::fmt::Write;
use ds::RingSlice;
use protocol::Error;
use sharding::{distribution::DBRange, hash::Hasher};

#[derive(Clone, Debug)]
pub struct User {
    db_prefix: String,
    table_prefix: String,
    db_postfix: String,
    table_postfix: String,
    hasher: Hasher,
    dist: DBRange,
    keys_name: Vec<String>,
}

impl User {
    pub fn new(
        db_prefix: String,
        table_prefix: String,
        db_postfix: String,
        table_postfix: String,
        db_count: u32,
        table_count: u32,
        keys_name: Vec<String>,
    ) -> Self {
        Self {
            db_prefix,
            table_prefix,
            db_postfix,
            table_postfix,
            //dbcount = 分片数量
            dist: DBRange::new_user(db_count as usize, table_count as usize),
            hasher: Hasher::from("crc32"),
            keys_name,
        }
    }

    pub fn distribution(&self) -> &DBRange {
        &self.dist
    }

    pub fn hasher(&self) -> &Hasher {
        &self.hasher
    }

    pub(crate) fn get_hex(n: usize) -> char {
        let n = (n & 0xf) as u8;
        if n < 10 {
            return (n + b'0') as char;
        }
        (n - 10 + b'a') as char
    }

    pub fn write_database_table(&self, buf: &mut impl Write, hash: i64) {
        let Self {
            db_prefix,
            table_prefix,
            db_postfix,
            table_postfix,
            dist,
            ..
        } = self;
        let mut db_hex = char::default();
        if db_postfix.is_empty() {
            let _ = buf.write_str(db_prefix);
        } else {
            db_hex = Self::get_hex(dist.db_idx(hash));
            let _ = write!(buf, "{db_prefix}_{db_hex}");
        }
        let _ = buf.write_char('.');
        if table_postfix.is_empty() {
            let _ = buf.write_str(table_prefix);
        } else {
            let table_hex = Self::get_hex(dist.table_idx(hash));
            let _ = write!(buf, "{table_prefix}_{db_hex}{table_hex}");
        }
    }

    pub(crate) fn keys(&self) -> &[String] {
        &self.keys_name
    }

    pub(crate) fn condition_keys(
        &self,
        keys: &Vec<RingSlice>,
        mut f: impl FnMut(bool, &String, &RingSlice),
    ) -> bool {
        let key_val = self.keys_name.iter().map(|k| Some(k)).zip(keys);

        let mut has_key = false;
        for (key, val) in key_val {
            if let Some(key) = key {
                f(!has_key, key, val);
                has_key = true;
            }
        }
        return has_key;
    }

    //虽然不分年库，先统一使用2000年
    pub(crate) fn get_date(&self, _keys: &[RingSlice]) -> Result<NaiveDate, Error> {
        const YEAR: Option<NaiveDate> = NaiveDate::from_ymd_opt(2000, 1, 1);
        Ok(YEAR.unwrap())
    }
}