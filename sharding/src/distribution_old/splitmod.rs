use super::DIST_SPLIT_MOD_WITH_SLOT_PREFIX;

// 算法： hash/split_count%split_count%sharding
#[derive(Clone, Debug, Default)]
pub struct SplitMod {
    split_count: u64,
    shard_count: u64,
}

impl SplitMod {
    pub fn from(name: &str, shards: usize) -> Self {
        assert!(name.len() > DIST_SPLIT_MOD_WITH_SLOT_PREFIX.len());
        assert!(name.starts_with(DIST_SPLIT_MOD_WITH_SLOT_PREFIX));

        // 根据算法，默认采用32
        let mut split = 32;
        let slot_str = &name[DIST_SPLIT_MOD_WITH_SLOT_PREFIX.len()..];
        if let Ok(s) = slot_str.parse::<u64>() {
            split = s;
        } else {
            log::warn!("found unknown distribution: {}, will use range", name);
        }

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
