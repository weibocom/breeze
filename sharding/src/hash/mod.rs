pub mod bkdr;
pub mod crc32;

pub use bkdr::Bkdr;
pub use crc32::Crc32;

use enum_dispatch::enum_dispatch;
#[enum_dispatch]
pub trait Hash {
    fn hash(&self, key: &[u8]) -> u64;
}

#[enum_dispatch(Hash)]
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
}
