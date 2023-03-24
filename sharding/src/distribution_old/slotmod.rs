// 算法： hash % slot_count % shard_count
#[derive(Clone, Debug, Default)]
pub struct SlotMod {
    slot_count: u64,
    shard_count: u64,
}

impl SlotMod {
    pub fn from(name: &str, shards: usize) -> Self {
        assert!(name.starts_with(super::DIST_SLOT_MOD_PREFIX));
        assert!(name.len() > super::DIST_SLOT_MOD_PREFIX.len());

        let slot = match name[super::DIST_SLOT_MOD_PREFIX.len()..].parse::<u64>() {
            Ok(s) => s,
            Err(_e) => {
                log::error!("slotmod - found malformed dist: {}, e: {:?}", name, _e);
                1024 // 默认1024个slot，保持与业务相同
            }
        };
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
