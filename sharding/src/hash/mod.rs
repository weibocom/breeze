// 数据分布，涉及到hash、distribution，同时对于hash会存在对标准hash做各种经验性调整优化；
// 这些调整优化算法，可以有3种处理姿势：
// 1. 放在hash层，这样一种crc32可能派生出多种hash算法，跟不同资源相关；
// 2. 放在distibution层，不足是也会派生出多种非标准的分布算法；
// 3. 放在独立层，先根据标准hash计算的原始值，然后独立层进行经验优化调整，然后再调用标准distribution层。
// 为了方便兼容和对比，先按方案1开发，待完成后，再考虑采用方案3？  fishermen 2021.12

pub mod bkdr;
pub mod crc32;

pub use bkdr::Bkdr;

use enum_dispatch::enum_dispatch;

use self::crc32::{Crc32Range, Crc32Short};

#[enum_dispatch]
pub trait Hash {
    fn hash(&self, key: &[u8]) -> u64;
}

#[enum_dispatch(Hash)]
pub enum Hasher {
    Bkdr(Bkdr),
    Crc32Short(Crc32Short), // mc short crc32
    Crc32Range(Crc32Range), // redis crc32 range hash
}

// crc32-short和crc32-range长度相同，所以此处选一个
const CRC32_BASE_LEN: usize = "crc32-range".len();
const CRC32_RANGE_ID_PREFIX_LEN: usize = "crc32-range-id-".len();

impl Hasher {
    pub fn from(alg: &str) -> Self {
        let alg_lower = alg.to_ascii_lowercase();
        let mut alg_match = alg_lower.as_str();
        if alg_match.len() > CRC32_BASE_LEN {
            alg_match = &alg_match[0..CRC32_BASE_LEN];
        }

        match alg_match {
            "bkdr" => Self::Bkdr(Default::default()),
            "crc32-short" => Self::Crc32Short(Default::default()),
            "crc32-range" => Self::Crc32Range(Crc32Range::from(alg_lower.as_str())),
            _ => {
                // 默认采用mc的crc32-s hash
                log::warn!("found unknow hash:{}, use crc32-short instead", alg);
                Self::Crc32Short(Default::default())
            } // _ => Self::Crc32(Default::default()),
        }
    }
}
