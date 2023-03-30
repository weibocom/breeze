//use super::DIST_SPLIT_MOD_WITH_SLOT_PREFIX;

// 算法： hash/split_count%split_count%sharding
#[derive(Clone, Debug, Default)]
pub struct SplitMod {
    split_count: u64,
    shard_count: u64,
}

impl SplitMod {
    pub fn from(num: Option<u64>, shards: usize) -> Self {
        // 根据算法，默认采用32
        let split = num.unwrap_or(32);

        assert!(shards > 0 && split >= shards as u64);
        SplitMod {
            split_count: split,
            shard_count: shards as u64,
        }
    }

    pub fn index(&self, hash: i64) -> usize {
        let mut val = hash
            .wrapping_div(self.split_count as i64)
            .wrapping_rem(self.split_count as i64);
        if val < 0 {
            log::warn!("found negative splitmod pre hash:{:?}", val);
            val = val.wrapping_abs();
        }
        let rs = val as u64 % self.shard_count;
        rs as usize
    }
}
