use crate::distribution::DIST_RANGE_SLOT_COUNT_DEFAULT;

// alg: hash/slot%slot/(slot/shards.len)
// 按区间进行分布，比如[0,16)分区，每段间隔是4，则分为4个区间：[0,4),[4,8),[8,12),[12,16)
#[derive(Clone, Debug, Default)]
pub struct Range {
    slot: u64,
    interval: u64,
}

// Range 分布方法，默认总范围是[0,256)，否则用
impl Range {
    pub fn from(slot: Option<u64>, shards: usize) -> Self {
        let slot = slot.unwrap_or(DIST_RANGE_SLOT_COUNT_DEFAULT);
        assert!(shards > 0 && slot >= shards as u64);
        Range {
            slot,
            interval: slot / shards as u64,
        }
    }

    pub fn index(&self, hash: i64) -> usize {
        let mut val = hash
            .wrapping_div(self.slot as i64)
            .wrapping_rem(self.slot as i64);
        if val < 0 {
            log::warn!("found negative crc range pre hash:{:?}", val);
            val = val.wrapping_abs();
        }
        let rs = val as u64 / self.interval;
        rs as usize
    }
}
