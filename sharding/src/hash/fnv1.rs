/// 按需支持fnv1系列所有相关的hash算法，目前支持fnv1_32、fnv1a_64;
/// 对应算法源自twemproxy

#[derive(Debug, Default, Clone)]
pub struct Fnv1F32;

const FNV_32_INIT: u32 = 2166136261;
const FNV_32_PRIME: u32 = 16777619;

impl super::Hash for Fnv1F32 {
    fn hash<S: crate::HashKey>(&self, key: &S) -> i64 {
        let mut hash = FNV_32_INIT;
        for i in 0..key.len() {
            hash = hash.wrapping_mul(FNV_32_PRIME);
            hash = hash ^ (key.at(i) as u32);
        }
        hash as i64
    }
}

#[derive(Debug, Default, Clone)]
pub struct Fnv1aF64;

const FNV_64_INIT: u64 = 0xcbf29ce484222325;
const FNV_64_PRIME: u64 = 0x100000001b3;

impl super::Hash for Fnv1aF64 {
    fn hash<S: crate::HashKey>(&self, key: &S) -> i64 {
        let mut hash = FNV_64_INIT as u32;
        for i in 0..key.len() {
            hash ^= key.at(i) as u32;
            hash = hash.wrapping_mul(FNV_64_PRIME as u32);
        }
        hash as i64
    }
}
