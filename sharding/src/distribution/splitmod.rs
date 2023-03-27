use super::DivMod;
use super::DIST_SPLIT_MOD_WITH_SLOT_PREFIX;

// 算法： hash/split_count%split_count%sharding
pub(super) fn div_mod(name: &str, shards: usize) -> DivMod {
    debug_assert!(shards > 0);
    debug_assert!(name.starts_with(DIST_SPLIT_MOD_WITH_SLOT_PREFIX));
    let split = name[DIST_SPLIT_MOD_WITH_SLOT_PREFIX.len()..]
        .parse::<usize>()
        .unwrap_or(32);
    // 因为split与shard count都是2的幂，所以取小值取模即可
    let m = split.min(shards);
    DivMod::pow(split, m, 1, false)
}
