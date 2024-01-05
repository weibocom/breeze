// #[derive(Debug, Clone)]
// pub struct Sharding {
//     hash: Hasher,
//     distribution: Distribute,
//     num: usize,
// }

pub mod hash;
use hash::*;

pub mod distribution;
// use distribution::*;

// use std::ops::Deref;

// impl Sharding {
// dead code 暂时注释掉
// pub fn from(hash_alg: &str, distribution: &str, names: Vec<String>) -> Self {
//     let num = names.len();
//     let h = Hasher::from(hash_alg);
//     let d = Distribute::from(distribution, &names);
//     Self {
//         hash: h,
//         distribution: d,
//         num: num,
//     }
// }
// #[inline]
// pub fn sharding(&self, key: &[u8]) -> usize {
//     let hash = self.hash.hash(&key);
//     let idx = self.distribution.index(hash);
//     assert!(idx < self.num);
//     idx
// }
// // key: sharding idx
// // value: 是keys idx列表
// #[inline]
// pub fn shardings<K: Deref<Target = [u8]>>(&self, keys: Vec<K>) -> Vec<Vec<usize>> {
//     let mut shards = vec![Vec::with_capacity(8); self.num];
//     for (ki, key) in keys.iter().enumerate() {
//         let idx = self.sharding(key);
//         unsafe { shards.get_unchecked_mut(idx).push(ki) };
//     }
//     shards
// }
// }
