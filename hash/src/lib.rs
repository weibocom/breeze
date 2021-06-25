mod bkdr;
use bkdr::Bkdr;

mod crc32;

use enum_dispatch::enum_dispatch;
#[enum_dispatch]
pub trait Hash {
    fn hash(&mut self, key: &[u8]) -> u64;
}

#[enum_dispatch(Hash)]
pub enum Hasher {
    SipHasher13(DefaultHasher),
    Bkdr(Bkdr),
}

impl Hasher {
    pub fn from(alg: &str) -> Self {
        match alg {
            "bkdr" | "BKDR" => Self::Bkdr(Default::default()),
            _ => Self::SipHasher13(DefaultHasher),
        }
    }
}

pub struct DefaultHasher;

impl DefaultHasher {
    pub fn new() -> Self {
        DefaultHasher
    }
}

impl Hash for DefaultHasher {
    fn hash(&mut self, key: &[u8]) -> u64 {
        use std::hash::Hasher;
        let mut hash = std::collections::hash_map::DefaultHasher::default();
        hash.write(key);
        hash.finish()
    }
}
