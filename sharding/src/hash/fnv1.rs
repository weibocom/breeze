/// 按需支持fnv1系列所有相关的hash算法，目前支持fnv1a_64

#[derive(Debug, Default, Clone)]
pub struct Fnv1a64;

const FNV_64_INIT: u64 = 0xcbf29ce484222325;
const FNV_64_PRIME: u64 = 0x100000001b3;

impl super::Hash for Fnv1a64 {
    fn hash<S: crate::HashKey>(&self, key: &S) -> i64 {
        let mut hash = FNV_64_INIT as u32;
        for i in 0..key.len() {
            hash ^= key.at(i) as u32;
            hash = hash.wrapping_mul(FNV_64_PRIME as u32);
        }
        log::debug!("+++ use Fnv1a64");
        hash as i64
    }
}
