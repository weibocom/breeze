pub trait Router {
    fn route(&mut self, req: &[u8]) -> usize;
}

#[enum_dispatch]
pub trait Hash {
    fn hash(&mut self, key: &[u8]) -> u64;
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

use enum_dispatch::enum_dispatch;

#[enum_dispatch(Hash)]
pub enum Hasher {
    Default(DefaultHasher),
}

impl Hasher {
    pub fn from(alg: &str) -> Self {
        match alg {
            _ => Self::Default(DefaultHasher),
        }
    }
}
