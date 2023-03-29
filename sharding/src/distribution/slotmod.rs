// 算法： hash % slot_count % shard_count
#[derive(Clone, Debug, Default)]
pub struct SlotMod {
    slot_count: u64,
    shard_count: u64,
}

impl SlotMod {
    pub fn from(num: Option<u64>, shards: usize) -> Self {
        let slot = num.unwrap_or(1024);
        SlotMod {
            slot_count: slot,
            shard_count: shards as u64,
        }
    }

    pub fn index(&self, hash: i64) -> usize {
        let idx = hash
            .wrapping_rem(self.slot_count as i64)
            .wrapping_rem(self.shard_count as i64);

        idx as usize
    }
}
