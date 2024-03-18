// 算法： hash % slot_count / slot_per_shard
#[derive(Clone, Debug, Default)]
pub struct SlotShard {
    slot_count: u64,
    slot_per_shard: u64, // shard内的slot数量
}

impl SlotShard {
    pub fn from(num: Option<u64>, shards: usize) -> Self {
        let slot = num.unwrap_or(1024);
        SlotShard {
            slot_count: slot,
            slot_per_shard: slot.wrapping_div(shards as u64),
        }
    }

    pub fn index(&self, hash: i64) -> usize {
        let idx = hash
            .wrapping_rem(self.slot_count as i64)
            .wrapping_div(self.slot_per_shard as i64);

        idx as usize
    }
}
