use chrono::{Datelike, TimeZone};
use chrono_tz::Asia::Shanghai;
// use std::collections::HashMap;
// use std::sync::atomic::{AtomicI64, Ordering};

pub struct UuidConst;
impl UuidConst {
    pub const ID_OFFSET: i64 = 515483463;

    pub const SEQ_BIT_LENGTH: i64 = 18;
    pub const IDC_SEQ_BIT_LENGTH: i64 = 4 + Self::SEQ_BIT_LENGTH;

    // 下面的这些变量目前代码里没用到
    // pub const ID_OFFSET_2014: i64 = 1388505600; //2014-01-01 00:00:00

    // pub const SEQ_LIMIT: i64 = 1 << Self::SEQ_BIT_LENGTH;
    // pub const SPEC_HA_BIT_LENGTH: i64 = 1;
    // pub const SPEC_SEQ_HA_BIT_LENGTH: i64 = 15 + Self::SPEC_HA_BIT_LENGTH;
    // pub const SPEC_BIZ_SEQ_HA_BIT_LENGTH: i64 = 2 + Self::SPEC_SEQ_HA_BIT_LENGTH;
    // pub const SPEC_IDC_BIZ_SEQ_HA_BIT_LENGTH: i64 = 4 + Self::SPEC_BIZ_SEQ_HA_BIT_LENGTH;
    // pub const SPEC_SEQ_LIMIT: i64 = 1 << (Self::SPEC_SEQ_HA_BIT_LENGTH - Self::SPEC_HA_BIT_LENGTH);
    // pub const SPEC_IDC_FLAGS: [i64; 5] = [2, 3, 12, 13, 14];
}

pub trait Uuid {
    // return UNIX timestamp
    fn unix_secs(self) -> i64;
    // 返回year month day.
    fn ymd(&self) -> (u16, u8, u8);
    // 返回year
    fn year(&self) -> u16;
}

impl Uuid for i64 {
    #[inline]
    fn unix_secs(self) -> i64 {
        (self >> UuidConst::IDC_SEQ_BIT_LENGTH) + UuidConst::ID_OFFSET
    }
    // 返回year month day. 东八时区
    #[inline]
    fn ymd(&self) -> (u16, u8, u8) {
        // 返回东八时区的DateTime
        let t = chrono::Utc
            .timestamp_opt(self.unix_secs(), 0)
            .unwrap()
            .with_timezone(&Shanghai)
            .naive_local();
        (t.year() as u16, t.month() as u8, t.day() as u8)
    }
    // 返回year 东八时区
    #[inline(always)]
    fn year(&self) -> u16 {
        let (year, _, _) = self.ymd();
        year
    }
}

pub trait UuidGet {
    fn uuid(&self) -> i64;
}

impl UuidGet for ds::RingSlice {
    #[inline(always)]
    fn uuid(&self) -> i64 {
        self.str_num(..) as i64
    }
}
