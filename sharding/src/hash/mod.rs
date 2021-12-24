pub mod bkdr;
pub mod crc32;

pub use bkdr::Bkdr;
pub use crc32::*;

use enum_dispatch::enum_dispatch;
#[enum_dispatch]
pub trait Hash {
    fn hash<S: HashKey>(&self, key: &S) -> u64;
}

#[enum_dispatch(Hash)]
#[derive(Debug, Clone)]
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
    #[inline(always)]
    pub fn crc32_short() -> Self {
        Self::Crc32Short(Default::default())
    }
}

impl Default for Hasher {
    #[inline]
    fn default() -> Self {
        Self::crc32_short()
    }
}

pub trait HashKey {
    fn len(&self) -> usize;
    fn at(&self, idx: usize) -> u8;
    fn vec_data(&self) -> Vec<u8>;
}

impl HashKey for &[u8] {
    #[inline(always)]
    fn len(&self) -> usize {
        (*self).len()
    }
    #[inline(always)]
    fn at(&self, idx: usize) -> u8 {
        unsafe { *self.as_ptr().offset(idx as isize) }
    }
    #[inline(always)]
    fn vec_data(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(self.len());
        data.extend(*self);
        data
    }
}

impl HashKey for ds::RingSlice {
    #[inline(always)]
    fn len(&self) -> usize {
        (*self).len()
    }
    #[inline(always)]
    fn at(&self, idx: usize) -> u8 {
        (*self).at(idx)
    }
    #[inline(always)]
    fn vec_data(&self) -> Vec<u8> {
        self.to_vec()
    }
}
