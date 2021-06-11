use std::collections::hash_map::DefaultHasher;
#[derive(Clone)]
pub enum Hasher {
    SipHasher13(DefaultHasher),
}

impl std::hash::Hasher for Hasher {
    fn write(&mut self, bytes: &[u8]) {
        match self {
            Self::SipHasher13(sip) => sip.write(bytes),
        }
    }
    fn finish(&self) -> u64 {
        match self {
            Self::SipHasher13(sip) => sip.finish(),
        }
    }
}

impl Default for Hasher {
    fn default() -> Self {
        Hasher::SipHasher13(DefaultHasher::new())
    }
}
