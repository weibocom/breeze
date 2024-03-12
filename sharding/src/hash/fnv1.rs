/// 按需支持fnv1系列所有相关的hash算法，目前支持fnv1_32, 还有一个没合并的mr支持fnv1a_64，merge时合并;
/// 对应算法源自twemproxy

#[derive(Debug, Default, Clone)]
pub struct Fnv1_32;

const FNV_32_INIT: u32 = 2166136261;
const FNV_32_PRIME: u32 = 16777619;

impl super::Hash for Fnv1_32 {
    fn hash<S: crate::HashKey>(&self, key: &S) -> i64 {
        let mut hash = FNV_32_INIT;
        for i in 0..key.len() {
            hash = hash.wrapping_mul(FNV_32_PRIME);
            hash = hash ^ (key.at(i) as u32);
        }
        hash as i64
    }
}
