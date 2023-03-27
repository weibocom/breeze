use super::DivMod;
// 算法： hash % slot_count % shard_count
pub(super) fn div_mod(name: &str, shards: usize) -> DivMod {
    debug_assert!(shards > 0);
    debug_assert!(name.starts_with(super::DIST_SLOT_MOD_PREFIX));
    let slot = name[super::DIST_SLOT_MOD_PREFIX.len()..]
        .parse::<usize>()
        .unwrap_or(1024);
    // 因为slot与shard count都是2的幂，所以取小值取模即可
    let m = slot.min(shards);
    println!("name:{} shards:{} => {}", name, shards, m);
    DivMod::pow(1, m, 1, false)
}
