use crate::distribution_old::DIST_MOD_RANGE_WITH_SLOT_PREFIX;

// // alg: hash%slot/(slot/shards.len)
// 先对slot取模，然后再按区间进行分布，比如[0,16)分区，每段间隔是4，则分为4个区间：[0,4),[4,8),[8,12),[12,16)
#[derive(Clone, Debug, Default)]
pub struct ModRange {
    slot: u64,
    interval: u64,
}

// ModRange 分布方法，默认总范围是[0,256)，否则指定slot
impl ModRange {
    pub fn from(name: &str, shards: usize) -> Self {
        assert!(name.len() > DIST_MOD_RANGE_WITH_SLOT_PREFIX.len());
        assert!(name.starts_with(DIST_MOD_RANGE_WITH_SLOT_PREFIX));

        let mut slot = 256;
        let slot_str = &name[DIST_MOD_RANGE_WITH_SLOT_PREFIX.len()..];
        if let Ok(s) = slot_str.parse::<u64>() {
            slot = s;
        } else {
            log::warn!("found unknown distribution: {}, will use range", name);
        }

        assert!(shards > 0 && slot >= shards as u64);
        ModRange {
            slot,
            interval: slot / shards as u64,
        }
    }

    pub fn index(&self, hash: i64) -> usize {
        let mut val = hash.wrapping_rem(self.slot as i64);
        if val < 0 {
            log::warn!("found negative modrange pre hash:{:?}", val);
            val = val.wrapping_abs();
        }
        let rs = val as u64 / self.interval;
        rs as usize
    }
}
