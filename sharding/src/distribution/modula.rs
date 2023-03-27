use crate::distribution::DivMod;

#[derive(Clone, Debug)]
pub struct ModulaNoPow2 {
    len: usize,
    absolute_hash: bool,
}

pub(super) fn div_mod(shards: usize, absolute_hash: bool) -> DivMod {
    debug_assert!(shards > 0);
    DivMod::pow(1, shards, 1, absolute_hash)
}

impl ModulaNoPow2 {
    pub fn from(shard_num: usize, absolute_hash: bool) -> Self {
        Self {
            len: shard_num,
            absolute_hash,
        }
    }
    #[inline]
    pub fn index(&self, hash: i64) -> usize {
        if self.absolute_hash {
            return (hash as i32).abs() as usize % self.len;
        } else {
            hash as usize % self.len
        }
    }
}
