// use chrono::{DateTime, TimeZone, Utc};
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
}

impl Uuid for i64 {
    fn unix_secs(self) -> i64 {
        (self >> UuidConst::IDC_SEQ_BIT_LENGTH) + UuidConst::ID_OFFSET
    }
}

// pub struct UuidHelper;

// impl UuidHelper {
//     pub const TIME_BIT: i64 = i64::MAX << UuidConst::IDC_SEQ_BIT_LENGTH;
//     pub const IDC_ID_BIT: i64 = 15 << UuidConst::SEQ_BIT_LENGTH;
//     pub const BIZ_FLAG_BIT: i64 = 3 << UuidConst::SPEC_SEQ_HA_BIT_LENGTH;

//     pub const TIME1_BIT: i64 = i64::MAX << 34; // 30个1+34个0
//     pub const TIME2_BIT: i64 = 1073741823 << 4; // 30个11+4个0
//     pub const FLAG_BIT: i64 = 15; //4个1

//     pub const MIN_VALID_ID: i64 = 3000000000000000;
//     // max id 支撑到2059.8.15，如果这段代码能延续到微博50周年，届时就继续扩大该ID（前值为45000...）。
//     pub const MAX_VALID_ID: i64 = 9700000000000000;

//     // pub fn is_uuid_after_update(id: i64) -> bool {
//     UuidHelper::is_valid_id(id) && id > 3342818919841793 // 微博id升级后的一个id, 2011-08-05 00:00:00
// }

// pub fn is_comment_mid_after_update(id: i64) -> bool {
//     UuidHelper::is_valid_id(id) && id > 3557855061995026 //评论mid改造后的一个id，2013-3-20 09:16:50
// }
// pub fn is_valid_id(id: i64) -> bool {
//     id > UuidHelper::MIN_VALID_ID && id < UuidHelper::MAX_VALID_ID
// }
// pub fn get_time(id: i64) -> i64 {
//     UuidHelper::get_time_number(id) + UuidConst::ID_OFFSET
// }
// pub fn get_time_number(id: i64) -> i64 {
//     id >> UuidConst::IDC_SEQ_BIT_LENGTH
// }
// pub fn get_idc(id: i64) -> i64 {
//     (id & UuidHelper::IDC_ID_BIT) >> UuidConst::SEQ_BIT_LENGTH
// }
// pub fn get_seq(id: i64) -> i64 {
//     if UuidHelper::is_spec_uuid(id) {
//         (id >> 1) % UuidConst::SPEC_SEQ_LIMIT
//     } else {
//         id % UuidConst::SEQ_LIMIT
//     }
// }
// pub fn unix_time(id: i64) -> i64 {
//     (id + UuidConst::ID_OFFSET_2014) * 1000
// }
// pub fn get_time_30(id: i64) -> i64 {
//     let actual_id = id / 1000 - UuidConst::ID_OFFSET_2014;
//     if actual_id > 0 {
//         actual_id
//     } else {
//         0
//     }
// }
// //get biz flag for spec uuid
// pub fn get_biz(id: i64) -> i64 {
//     if Self::is_spec_uuid(id) {
//         return (id & Self::BIZ_FLAG_BIT) >> UuidConst::SPEC_SEQ_HA_BIT_LENGTH;
//     }
//     return -1;
// }
// pub fn is_spec_idc(idc_flag: i64) -> bool {
//     for spec_idc in UuidConst::SPEC_IDC_FLAGS {
//         if spec_idc == idc_flag {
//             return true;
//         }
//     }
//     return false;
// }
// //check if the uuid is spec uuid
// fn is_spec_uuid(id: i64) -> bool {
//     let idc_id = Self::get_idc(id);
//     return Self::is_spec_idc(idc_id);
// }

// //get id by date
// pub fn get_id(date: DateTime<Utc>) -> i64 {
//     return UuidSimulator::new().generate_id(date.timestamp_millis());
// }
// //get min id by date
// pub fn get_min_id(date: DateTime<Utc>) -> i64 {
//     return UuidSimulator::new().generate_min_id(date.timestamp_millis());
// }
// //get max id by date
// pub fn get_max_id(date: DateTime<Utc>) -> i64 {
//     return UuidSimulator::new().generate_max_id(date.timestamp_millis());
// }
// //get timestamp from id with second precision
// pub fn get_timestamp_from_id(id: i64) -> i64 {
//     return (Self::get_time_number(id) + UuidConst::ID_OFFSET) * 1000;
// }
// }
// pub struct UuidSimulator {
//     idc_id: i64,
//     ha_id: i32,
//     seq: AtomicI64,
//     spec_seq_ids: HashMap<String, AtomicI64>,
// }

// impl UuidSimulator {
//     pub const IDC_ID_FOR_DEFAULT_UUID: i64 = 6; // 0-15(default use 0-11, not-default use 12-13)
//     pub const IDC_ID_FOR_SPEC_UUID: i64 = 13;
//     pub const IDC_ID_MAX: i64 = 15;
//     pub const ID_INTERVAL: i64 = 1;
//
//     pub fn new() -> Self {
//         let mut spec_seq_ids = HashMap::new();
//         if Self::is_spec_idc(Self::IDC_ID_FOR_DEFAULT_UUID) {
//             for biz_flag in &[BizFlag::Activity] {
//                 spec_seq_ids.insert(biz_flag.get_value(), AtomicI64::new(0));
//             }
//         }
//         Self {
//             idc_id: 1,
//             ha_id: 1,
//             seq: AtomicI64::new(0),
//             spec_seq_ids,
//         }
//     }
//     pub fn from(idc: i64) -> Self {
//         let mut spec_seq_ids = HashMap::new();
//         if UuidHelper::is_spec_idc(idc) {
//             for biz_flag in &[
//                 BizFlag::Activity,
//                 BizFlag::Api,
//                 BizFlag::DefaultBiz,
//                 BizFlag::Video,
//             ] {
//                 spec_seq_ids.insert(biz_flag.get_value(), AtomicI64::new(0));
//             }
//         }
//         Self {
//             idc_id: idc,
//             ha_id: 1,
//             seq: AtomicI64::new(0),
//             spec_seq_ids,
//         }
//     }

//     pub fn generate_id(&self, time: i64) -> i64 {
//         Self::assemble_id(
//             time,
//             self.idc_id,
//             self.seq.fetch_add(Self::ID_INTERVAL, Ordering::SeqCst),
//         )
//     }

//     pub fn generate_min_id(&self, time: i64) -> i64 {
//         Self::assemble_id(time, 0, 0)
//     }
//     pub fn generate_max_id(&self, time: i64) -> i64 {
//         Self::assemble_id(time, UuidSimulator::IDC_ID_MAX, UuidConst::SEQ_LIMIT - 1)
//     }

//     pub fn generate_spec_id(&self, time: i64, biz_flag: BizFlag) -> i64 {
//         if biz_flag == BizFlag::Activity && !UuidHelper::is_spec_idc(self.idc_id) {
//             panic!(
//                 "Cannot generate spec ids, for bizFlag={:?}, idc={}, isSpecUuidSimulator={}",
//                 biz_flag.get_value(),
//                 self.idc_id,
//                 UuidHelper::is_spec_idc(self.idc_id)
//             );
//         }
//         let biz_flag_num = biz_flag.get_num();
//         let seq =
//             self.spec_seq_ids[&biz_flag.get_value()].fetch_add(Self::ID_INTERVAL, Ordering::SeqCst);
//         Self::assemble_spec_id(time, self.idc_id, biz_flag_num as i64, seq, self.ha_id)
//     }

//     fn assemble_id(time: i64, idc: i64, seq: i64) -> i64 {
//         let mut uuid = time / 1000;
//         uuid -= UuidConst::ID_OFFSET;
//         uuid <<= UuidConst::IDC_SEQ_BIT_LENGTH;
//         uuid += idc << UuidConst::SEQ_BIT_LENGTH;
//         uuid += seq % UuidConst::SEQ_LIMIT;
//         uuid
//     }

//     fn assemble_spec_id(time: i64, idc: i64, biz_flag: i64, seq: i64, ha_id: i32) -> i64 {
//         let mut uuid = time / 1000;
//         uuid -= UuidConst::ID_OFFSET;
//         uuid <<= UuidConst::SPEC_IDC_BIZ_SEQ_HA_BIT_LENGTH;
//         uuid += idc << UuidConst::SPEC_BIZ_SEQ_HA_BIT_LENGTH;
//         uuid += biz_flag << UuidConst::SPEC_SEQ_HA_BIT_LENGTH;
//         uuid += (seq % UuidConst::SPEC_SEQ_LIMIT) << 1;
//         uuid += ha_id as i64;
//         uuid
//     }
// }

// #[derive(PartialEq)]
// pub enum BizFlag {
//     DefaultBiz,
//     Activity,
//     Api,
//     Video,
// }
// impl BizFlag {
//     pub fn parse_flag(flag_value: i32) -> Option<Self> {
//         match flag_value {
//             x if x == BizFlag::DefaultBiz as i32 => Some(BizFlag::DefaultBiz),
//             x if x == BizFlag::Activity as i32 => Some(BizFlag::Activity),
//             x if x == BizFlag::Api as i32 => Some(BizFlag::Api),
//             x if x == BizFlag::Video as i32 => Some(BizFlag::Video),
//             _ => None,
//         }
//     }
//     pub fn get_value(&self) -> String {
//         match self {
//             Self::DefaultBiz => String::from("default"),
//             Self::Activity => String::from("activity"),
//             Self::Api => String::from("api"),
//             Self::Video => String::from("video"),
//         }
//     }
//     pub fn get_num(&self) -> i32 {
//         match self {
//             Self::DefaultBiz => 0,
//             Self::Activity => 1,
//             Self::Api => 2,
//             Self::Video => 3,
//         }
//     }
// }

// impl BizFlag {}
