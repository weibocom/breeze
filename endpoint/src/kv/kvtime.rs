use super::{strategy::Postfix, uuid::*};
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
    pub fn new(name: String, db_count: u32, shards: u32, table_postfix: Postfix) -> Self {
        Self {
            db_prefix: name.clone(),
            table_prefix: name,
            table_postfix,
            distribution: DBRange::new(db_count as usize, 1usize, shards as usize),
            hasher: Hasher::from("crc32"),
        }
    }
    fn write_tname(&self, buf: &mut impl Write, key: &RingSlice) {
        let uuid = key.uuid();
        let (mut year, month, day) = uuid.ymd();
        year %= 100;
        let _ = write!(buf, "{}_{:02}{:02}", self.table_prefix, year, month);
        // 判断是否需要写入day
        match self.table_postfix {
            Postfix::YYMM => {}
            //Postfix::YYMMDD
            _ => write!(buf, "{:02}", day).expect("buf"),
        };
    }

    fn write_dname(&self, buf: &mut impl Write, key: &RingSlice) {
        let db_idx: usize = self.distribution.db_idx(self.hasher.hash(key));
        let _ = write!(buf, "{}_{}", self.db_prefix, db_idx);
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
        let uuid = key.uuid();
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
