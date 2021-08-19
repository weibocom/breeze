pub struct Sharding {
    hash: Hasher,
    distribution: Distribute,
}

mod hash;
use hash::*;

mod distribution;
use distribution::*;

impl Sharding {
    pub fn from(hash_alg: &str, distribution: &str, names: Vec<String>) -> Self {
        let h = Hasher::from(hash_alg);
        let d = Distribute::from(distribution, names);
        Self {
            hash: h,
            distribution: d,
        }
    }
    #[inline(always)]
    pub fn sharding(&self, key: &[u8]) -> usize {
        let hash = self.hash.hash(key);
        self.distribution.index(hash)
    }
}
