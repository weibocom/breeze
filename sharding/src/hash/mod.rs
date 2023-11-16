pub mod bkdr;
pub mod bkdrabscrc32;
pub mod bkdrsub;
pub mod crc32;
pub mod crc32local;
pub mod crc64;
pub mod lbcrc32local;
pub mod padding;
pub mod random;
pub mod raw;
pub mod rawcrc32local;
pub mod rawsuffix;

pub use bkdr::Bkdr;
pub use bkdrabscrc32::BkdrAbsCrc32;
pub use crc32::*;
pub use crc32local::*;
pub use lbcrc32local::LBCrc32localDelimiter;
pub use padding::Padding;
pub use random::RandomHash;
pub use raw::Raw;
pub use rawcrc32local::Rawcrc32local;
pub use rawsuffix::RawSuffix;

pub mod crc;

use enum_dispatch::enum_dispatch;

use self::{bkdrsub::Bkdrsub, crc64::Crc64};

// 占位hash，主要用于兼容服务框架，供mq等业务使用
pub const HASH_PADDING: &str = "padding";

// hash算法名称分隔符，合法的算法如：crc32,crc-short, crc32-num, crc32-point, crc32-pound, crc32-underscore
pub const HASHER_NAME_DELIMITER: char = '-';

// hash key是全部的key，但最后要做short截断
pub const CRC32_EXT_SHORT: &str = "short";
// hash key是数字
const CRC32_EXT_NUM: &str = "num";
// hash key是第一串长度大于等于5位数字id
const CRC32_EXT_SMARTNUM: &str = "smartnum";
// smart num的hashkey最小长度为5
const SMARTNUM_MIN_LEN: usize = 5;
// mixnum 的 hashkey 是所有key中num的混合
const CRC32_EXT_MIXNUM: &str = "mixnum";

// hash key是点号"."分割的之前/后的部分
const KEY_DELIMITER_POINT: &str = "point";
// hash key是“#”之前/后的部分
const KEY_DELIMITER_POUND: &str = "pound";
// hash key是"_"之前/后的部分
const KEY_DELIMITER_UNDERSCORE: &str = "underscore";

// 用于表示无分隔符的场景
const KEY_DELIMITER_NONE: u8 = 0;

const NAME_RAWSUFFIX: &str = "rawsuffix";

#[enum_dispatch]
pub trait Hash {
    // hash 可能返回负数
    fn hash<S: HashKey>(&self, key: &S) -> i64;
}

#[enum_dispatch(Hash)]
#[derive(Debug, Clone)]
pub enum Hasher {
    Padding(Padding),
    Raw(Raw), // redis raw, long型字符串直接用数字作为hash
    Bkdr(Bkdr),
    Bkdrsub(Bkdrsub),
    BkdrAbsCrc32(BkdrAbsCrc32), // 混合三种hash：先bkdr，再abs，最后进行crc32计算
    Crc32(Crc32),
    Crc32Short(Crc32Short),         // mc short crc32
    Crc32Num(Crc32Num),             // crc32 for a hash key whick is a num,
    Crc32SmartNum(Crc32SmartNum),   // crc32 for key like： xxx + id + xxx，id的长度需要大于等于5
    Crc32MixNum(Crc32MixNum),       // crc32 for key: xx_num1_num2_xx，所有num即hashkey(num1num2)
    Crc32Delimiter(Crc32Delimiter), // crc32 for a hash key which has a delimiter of "." or "_" or "#" etc.
    Crc32local(Crc32local),         // crc32local for a hash key like: xx.x, xx_x, xx#x etc.
    Crc32localDelimiter(Crc32localDelimiter),
    Crc32localSmartNum(Crc32localSmartNum), //crc32 for key like： xxx + id + xxx，id的长度需要大于等于5
    LBCrc32localDelimiter(LBCrc32localDelimiter), // long bytes crc32local for hash like: 123.a, 124_a, 123#a
    Rawcrc32local(Rawcrc32local),                 // raw or crc32local
    Crc32Abs(Crc32Abs), // crc32abs: 基于i32转换，然后直接取abs；其他走i64提升为正数
    Crc64(Crc64),       // Crc64 算法，对整个key做crc64计算
    Random(RandomHash), // random hash
    RawSuffix(RawSuffix),
}

impl Hasher {
    // 主要做3件事：1）将hash alg转为小写；2）兼容xx-range；3）兼容-id为-num
    fn reconcreate_hash_name(alg: &str) -> String {
        let mut alg_lower = alg.to_ascii_lowercase();

        // 如果alg带有range的hash名称（即crc32-range-xxx or crc32-range），需要去掉"-range"
        let range_flag = "-range";
        if alg_lower.contains(range_flag) {
            alg_lower = alg_lower.replace(range_flag, "");
            log::debug!("replace old range hash name/{} with {}", alg, alg_lower);
        }

        // 如果alg带有"-id"，需要把"-id"换为"-num"，like crc32-id => crc32-num
        let id_flag = "-id";
        if alg_lower.contains(id_flag) {
            alg_lower = alg_lower.replace(id_flag, "-num");
            log::debug!("replace old id hash name/{} with {}", alg, alg_lower);
        }

        alg_lower
    }
    pub fn from(alg: &str) -> Self {
        let alg_lower = Hasher::reconcreate_hash_name(alg);
        let alg_parts: Vec<&str> = alg_lower.split(HASHER_NAME_DELIMITER).collect();

        // 简单hash，即名字中没有"-"的hash，目前只有bkdr、raw、crc32
        if alg_parts.len() == 1 {
            return match alg_parts[0] {
                HASH_PADDING => Self::Padding(Default::default()),
                "bkdr" => Self::Bkdr(Default::default()),
                "bkdrsub" => Self::Bkdrsub(Default::default()),
                "bkdrabscrc32" => Self::BkdrAbsCrc32(Default::default()),
                "raw" => Self::Raw(Raw::from(Default::default())),
                "crc32" => Self::Crc32(Default::default()),
                "crc32local" => Self::Crc32local(Default::default()),
                "rawcrc32local" => Self::Rawcrc32local(Default::default()),
                "lbcrc32local" => {
                    Self::LBCrc32localDelimiter(LBCrc32localDelimiter::from(alg_lower.as_str()))
                }
                "crc32abs" => Self::Crc32Abs(Default::default()),
                "crc64" => Self::Crc64(Default::default()),
                "random" => Self::Random(Default::default()),
                _ => {
                    // 默认采用mc的crc32-s hash
                    log::error!("found unknown hash:{}, use crc32-short instead", alg);
                    return Self::Crc32Short(Default::default());
                }
            };
        }

        // 扩展hash，包括crc32扩展、crc32local扩展：
        // 1 crc32 扩展hash，目前包含3类：short、num、delimiter，前两种为：crc32-short, crc-32-num；
        //   crc32-delimiter包括各种可扩展的分隔符，like： crc32-point, crc32-pound,crc32-underscore；
        //   如果业务有固定前缀，也可以支持，在hash name后加-xxx，xxx为前缀长度。
        // 2 crc32local 扩展hash，包括各种可扩展的分隔符，like： crc32-point, crc32-pound,crc32-underscore；
        debug_assert!(alg_parts.len() == 2 || alg_parts.len() == 3);
        match alg_parts[0] {
            "crc32" => match alg_parts[1] {
                CRC32_EXT_SHORT => Self::Crc32Short(Default::default()),
                CRC32_EXT_NUM => Self::Crc32Num(Crc32Num::from(alg_lower.as_str())),
                CRC32_EXT_SMARTNUM => Self::Crc32SmartNum(Default::default()),
                CRC32_EXT_MIXNUM => Self::Crc32MixNum(Default::default()),
                _ => Self::Crc32Delimiter(Crc32Delimiter::from(alg_lower.as_str())),
            },
            "crc32local" => match alg_parts[1] {
                CRC32_EXT_SMARTNUM => Self::Crc32localSmartNum(Default::default()),
                _ => Self::Crc32localDelimiter(Crc32localDelimiter::from(alg_lower.as_str())),
            },
            "rawsuffix" => Self::RawSuffix(RawSuffix::from(alg_lower.as_str())),
            _ => {
                log::error!("found unknow hash: {} use crc32 instead", alg);
                Self::Crc32(Default::default())
            }
        }
    }
    #[inline]
    pub fn crc32_short() -> Self {
        Self::Crc32Short(Default::default())
    }
}

// 如果有新增的key分隔符，在这里增加即可
fn key_delimiter_name_2u8(_alg: &str, delimiter_name: &str) -> u8 {
    let c = match delimiter_name {
        KEY_DELIMITER_POINT => '.',
        KEY_DELIMITER_UNDERSCORE => '_',
        KEY_DELIMITER_POUND => '#',
        _ => {
            log::debug!("found unknown hash alg: {}, use crc32 instead", _alg);
            KEY_DELIMITER_NONE as char
        }
    };
    c as u8
}

impl Default for Hasher {
    #[inline]
    fn default() -> Self {
        Self::crc32_short()
    }
}

pub trait HashKey: std::fmt::Debug {
    fn len(&self) -> usize;
    fn at(&self, idx: usize) -> u8;
    #[inline]
    fn num(&self, oft: usize) -> (i64, Option<u8>) {
        let mut num = 0;
        for i in oft..self.len() {
            let c = self.at(i);
            if !c.is_ascii_digit() {
                return (num, Some(c));
            }
            num = num.wrapping_mul(10) + (c - b'0') as i64;
        }
        (num, None)
    }
    #[inline]
    fn find(&self, oft: usize, found: impl Fn(u8) -> bool) -> Option<usize> {
        for i in oft..self.len() {
            if found(self.at(i)) {
                return Some(i);
            }
        }
        None
    }
}

impl HashKey for &[u8] {
    #[inline]
    fn len(&self) -> usize {
        (*self).len()
    }
    #[inline]
    fn at(&self, idx: usize) -> u8 {
        unsafe { *self.as_ptr().offset(idx as isize) }
    }
}

impl HashKey for ds::RingSlice {
    #[inline]
    fn len(&self) -> usize {
        (*self).len()
    }
    #[inline]
    fn at(&self, idx: usize) -> u8 {
        (*self).at(idx)
    }
}

// 把所有的小写字母换成大写
#[derive(Debug)]
pub struct UppercaseHashKey<'a, T> {
    inner: &'a T,
}

impl<'a, T: HashKey> super::HashKey for UppercaseHashKey<'a, T> {
    #[inline]
    fn len(&self) -> usize {
        self.inner.len()
    }
    #[inline]
    fn at(&self, idx: usize) -> u8 {
        TO_UPPER_CASE_TABLE[self.inner.at(idx) as usize]
    }
}
impl<'a, T> UppercaseHashKey<'a, T> {
    #[inline]
    pub fn new(t: &'a T) -> Self {
        Self { inner: t }
    }
}

const TO_UPPER_CASE_TABLE: [u8; 256] = [
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
    0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
    0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f,
    0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f,
    0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49, 0x4a, 0x4b, 0x4c, 0x4d, 0x4e, 0x4f,
    0x50, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x5a, 0x5b, 0x5c, 0x5d, 0x5e, 0x5f,
    0x60, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49, 0x4a, 0x4b, 0x4c, 0x4d, 0x4e, 0x4f,
    0x50, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x5a, 0x7b, 0x7c, 0x7d, 0x7e, 0x7f,
    0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8a, 0x8b, 0x8c, 0x8d, 0x8e, 0x8f,
    0x90, 0x91, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9a, 0x9b, 0x9c, 0x9d, 0x9e, 0x9f,
    0xa0, 0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8, 0xa9, 0xaa, 0xab, 0xac, 0xad, 0xae, 0xaf,
    0xb0, 0xb1, 0xb2, 0xb3, 0xb4, 0xb5, 0xb6, 0xb7, 0xb8, 0xb9, 0xba, 0xbb, 0xbc, 0xbd, 0xbe, 0xbf,
    0xc0, 0xc1, 0xc2, 0xc3, 0xc4, 0xc5, 0xc6, 0xc7, 0xc8, 0xc9, 0xca, 0xcb, 0xcc, 0xcd, 0xce, 0xcf,
    0xd0, 0xd1, 0xd2, 0xd3, 0xd4, 0xd5, 0xd6, 0xd7, 0xd8, 0xd9, 0xda, 0xdb, 0xdc, 0xdd, 0xde, 0xdf,
    0xe0, 0xe1, 0xe2, 0xe3, 0xe4, 0xe5, 0xe6, 0xe7, 0xe8, 0xe9, 0xea, 0xeb, 0xec, 0xed, 0xee, 0xef,
    0xf0, 0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7, 0xf8, 0xf9, 0xfa, 0xfb, 0xfc, 0xfd, 0xfe, 0xff,
];
#[derive(Debug, Clone, Default)]
struct DebugName {}
impl From<&str> for DebugName {
    fn from(_name: &str) -> Self {
        DebugName {}
    }
}
