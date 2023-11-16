// 用于兼容api-commons中的Util.crc32(Longs.toByteArray(id))，一般尽量不用使用 fishermen

use std::fmt::Display;

use super::{
    crc32::{CRC32TAB, CRC_SEED},
    DebugName,
};

// LBCrc32local算法，需要先转为u64的bytes，然后再计算hash
#[derive(Default, Clone, Debug)]
pub struct LBCrc32localDelimiter {
    name: DebugName,
}

impl LBCrc32localDelimiter {
    pub fn from(alg: &str) -> Self {
        Self { name: alg.into() }
    }
}

impl super::Hash for LBCrc32localDelimiter {
    fn hash<S: super::HashKey>(&self, key: &S) -> i64 {
        // parse key to long bytes
        let mut hkey = 0u64;
        for i in 0..key.len() {
            let c = key.at(i);
            if !c.is_ascii_digit() {
                break;
            }
            hkey = hkey.wrapping_mul(10) + (key.at(i) - '0' as u8) as u64;
        }

        // java 的Longs.toByteArray 用big endian
        let hkey_bytes = hkey.to_be_bytes();
        let mut crc: i64 = CRC_SEED;
        for i in 0..hkey_bytes.len() {
            let c = hkey_bytes[i];
            crc = crc >> 8 ^ CRC32TAB[((crc ^ (c as i64)) & 0xff) as usize];
        }
        crc ^= CRC_SEED;
        let crc32 = crc as i32;

        crc32.abs() as i64
    }
}

impl Display for LBCrc32localDelimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.name)
    }
}
