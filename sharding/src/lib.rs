pub struct Sharding {
    hash: Hasher,
    distribution: Distribute,
    num: usize,
}

mod hash;
use hash::*;

mod distribution;
use distribution::*;

use std::collections::HashMap;

impl Sharding {
    pub fn from(hash_alg: &str, distribution: &str, names: Vec<String>) -> Self {
        let num = names.len();
        let h = Hasher::from(hash_alg);
        let d = Distribute::from(distribution, names);
        Self {
            hash: h,
            distribution: d,
            num: num,
        }
    }
    #[inline(always)]
    pub fn sharding(&self, key: &[u8]) -> usize {
        let hash = self.hash.hash(key);
        let idx = self.distribution.index(hash);
        debug_assert!(idx < self.num);
        idx
    }
    // key: sharding idx
    // value: 是keys idx列表
    #[inline]
    pub fn shardings(&self, keys: Vec<&[u8]>) -> HashMap<usize, Vec<usize>> {
        let mut shards: HashMap<usize, Vec<usize>> = HashMap::with_capacity(self.num);
        for (ki, key) in keys.iter().enumerate() {
            let idx = self.sharding(key);
            if let Some(shard) = shards.get_mut(&idx) {
                shard.push(ki);
            } else {
                let mut shard = Vec::with_capacity(keys.len());
                shard.push(ki);
                shards.insert(idx, shard);
            }
        }
        shards
    }
}
