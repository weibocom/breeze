mod bkdr;
use bkdr::Bkdr;

use std::collections::hash_map::DefaultHasher;


#[derive(Clone)]
pub enum Hasher {
    SipHasher13(DefaultHasher),
    Bkdr(Bkdr),
}

impl std::hash::Hasher for Hasher {
    fn write(&mut self, bytes: &[u8]) {
        match self {
            Self::SipHasher13(sip) => sip.write(bytes),
            Self::Bkdr(bkdr) => bkdr.write(bytes),
        }
    }
    fn finish(&self) -> u64 {
        match self {
            Self::SipHasher13(sip) => sip.finish(),
            Self::Bkdr(bkdr) => bkdr.finish(),
        }
    }
}

impl Default for Hasher {
    fn default() -> Self {
        // 默认hash改为bkdr
        //Hasher::SipHasher13(DefaultHasher::new())
        Hasher::Bkdr(Bkdr::new())
    }
}

impl Hasher {
    pub fn bkdr() -> Self {
        return Self::Bkdr(Bkdr::new());
    }
}
