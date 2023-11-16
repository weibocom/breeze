// 用于兼容api-commons中的Util.crc32()，一般尽量不用使用 fishermen

use std::fmt::Display;

use super::{
    crc32::{self, CRC32TAB, CRC_SEED},
    DebugName, Hash,
};

#[derive(Default, Clone, Debug)]
pub struct Crc32local {}

impl Hash for Crc32local {
    fn hash<S: super::HashKey>(&self, key: &S) -> i64 {
        let mut crc: i64 = CRC_SEED;

        for i in 0..key.len() {
            let c = key.at(i);
            crc = crc >> 8 ^ CRC32TAB[((crc ^ (c as i64)) & 0xff) as usize];
        }

        crc ^= CRC_SEED;
        let crc32 = crc as i32;
        crc32.abs() as i64
    }
}

// Crc32算法，hash key是开始位置之后、分隔符之前的字符串
#[derive(Default, Clone, Debug)]
pub struct Crc32localDelimiter {
    start_pos: usize,
    delimiter: u8,
    name: DebugName,
}

impl Crc32localDelimiter {
    pub fn from(alg: &str) -> Self {
        let alg_parts: Vec<&str> = alg.split(super::HASHER_NAME_DELIMITER).collect();

        debug_assert!(alg_parts.len() >= 2);
        debug_assert_eq!(alg_parts[0], "crc32local");

        let delimiter = super::key_delimiter_name_2u8(alg, alg_parts[1]);

        if alg_parts.len() == 2 {
            return Self {
                start_pos: 0,
                delimiter,
                name: alg.into(),
            };
        }

        debug_assert!(alg_parts.len() == 3);
        if let Ok(prefix_len) = alg_parts[2].parse::<usize>() {
            return Self {
                start_pos: prefix_len,
                delimiter,
                name: alg.into(),
            };
        } else {
            log::debug!("unknown crc32local hash/{}, ignore prefix instead", alg);
            return Self {
                start_pos: 0,
                delimiter,
                name: alg.into(),
            };
        }
    }
}

impl super::Hash for Crc32localDelimiter {
    fn hash<S: super::HashKey>(&self, key: &S) -> i64 {
        let mut crc: i64 = CRC_SEED;
        debug_assert!(self.start_pos < key.len());

        // 对于用“.”、“_”、“#”做分割的hash key，遇到分隔符停止
        let check_delimiter = self.delimiter != super::KEY_DELIMITER_NONE;
        for i in self.start_pos..key.len() {
            let c = key.at(i);
            if check_delimiter && (c == self.delimiter) {
                break;
            }
            crc = crc >> 8 ^ CRC32TAB[((crc ^ (c as i64)) & 0xff) as usize];
        }

        crc ^= CRC_SEED;
        let crc32 = crc as i32;

        crc32.abs() as i64
    }
}

impl Display for Crc32localDelimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.name)
    }
}

#[derive(Default, Clone, Debug)]
pub struct Crc32localSmartNum {}

impl super::Hash for Crc32localSmartNum {
    fn hash<S: super::HashKey>(&self, key: &S) -> i64 {
        // 解析出smartnum hashkey的位置
        let (start, end) = crc32::parse_smartnum_hashkey(key);

        let mut crc: i64 = CRC_SEED;
        for i in start..end {
            let c = key.at(i);
            // smartnum hash，理论上必须是全部数字，但非法请求可能包含非数字（或者配置错误）
            //debug_assert!(c.is_ascii_digit(), "malfromed smart key:{:?}", key);
            crc = crc >> 8 ^ CRC32TAB[((crc ^ (c as i64)) & 0xff) as usize];
        }

        crc ^= CRC_SEED;
        let crc32 = crc as i32;
        crc32.abs() as i64
    }
}
