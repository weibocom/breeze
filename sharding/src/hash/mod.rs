pub mod bkdr;
pub mod crc32;

pub use bkdr::Bkdr;
pub use crc32::Crc32;

use enum_dispatch::enum_dispatch;
#[enum_dispatch]
pub trait Hash {
    fn hash<S: HashKey>(&self, key: &S) -> u64;
}

#[enum_dispatch(Hash)]
#[derive(Clone)]
pub enum Hasher {
    Bkdr(Bkdr),
    Crc32(Crc32),
}

impl Hasher {
    pub fn from(alg: &str) -> Self {
        match alg {
            "bkdr" | "BKDR" => Self::Bkdr(Default::default()),
            "crc32" | "CRC32" => Self::Crc32(Default::default()),
            _ => Self::Crc32(Default::default()),
        }
    }
    #[inline(always)]
    pub fn crc32() -> Self {
        Self::Crc32(Default::default())
    }
}

impl Default for Hasher {
    #[inline]
    fn default() -> Self {
        Self::crc32()
    }
}

pub trait HashKey {
    fn len(&self) -> usize;
    fn at(&self, idx: usize) -> u8;
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
}
