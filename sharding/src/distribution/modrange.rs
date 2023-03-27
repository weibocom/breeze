use super::DivMod;
use crate::distribution::DIST_MOD_RANGE_WITH_SLOT_PREFIX;

// // alg: hash%slot/(slot/shards.len)
// 先对slot取模，然后再按区间进行分布，比如[0,16)分区，每段间隔是4，则分为4个区间：[0,4),[4,8),[8,12),[12,16)
pub(super) fn div_mod(name: &str, shards: usize) -> DivMod {
    debug_assert!(shards > 0);
    debug_assert!(name.starts_with(DIST_MOD_RANGE_WITH_SLOT_PREFIX));
    let slot = name[DIST_MOD_RANGE_WITH_SLOT_PREFIX.len()..]
        .parse::<usize>()
        .unwrap_or(256);
    // 不用除slot，直接为1
    DivMod::pow(1, slot, slot / shards, false)
}
