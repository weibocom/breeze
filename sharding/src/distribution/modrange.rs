//use crate::distribution::DIST_MOD_RANGE_WITH_SLOT_PREFIX;

// // alg: hash%slot/(slot/shards.len)
// 先对slot取模，然后再按区间进行分布，比如[0,16)分区，每段间隔是4，则分为4个区间：[0,4),[4,8),[8,12),[12,16)
#[derive(Clone, Debug, Default)]
pub struct ModRange {
    slot: u64,
    interval: u64,
}

// ModRange 分布方法，默认总范围是[0,256)，否则指定slot
impl ModRange {
    pub fn from(num: Option<u64>, shards: usize) -> Self {
        let slot = num.unwrap_or(256);

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
