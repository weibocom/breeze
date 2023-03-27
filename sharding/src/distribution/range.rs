use super::DivMod;
use crate::distribution::{DIST_RANGE, DIST_RANGE_SLOT_COUNT_DEFAULT, DIST_RANGE_WITH_SLOT_PREFIX};

// alg: hash/slot%slot/(slot/shards.len)
// 按区间进行分布，比如[0,16)分区，每段间隔是4，则分为4个区间：[0,4),[4,8),[8,12),[12,16)
pub fn div_mod(name: &str, shards: usize) -> DivMod {
    debug_assert!(
        shards > 0 || name.starts_with(DIST_RANGE_WITH_SLOT_PREFIX) || name.eq(DIST_RANGE),
        "{}",
        name
    );
    let slot = name[DIST_RANGE_WITH_SLOT_PREFIX.len().min(name.len())..]
        .parse::<usize>()
        .unwrap_or(DIST_RANGE_SLOT_COUNT_DEFAULT as usize);
    DivMod::pow(slot, slot, slot / shards, false)
}
