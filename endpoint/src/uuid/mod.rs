pub(super) mod config;
pub mod topo;

struct Context {
    runs: u16, // 运行的次数
    idx: u16,  //最多有65535个主从
    shard_idx: u16,
    _ignore: u16,
}

#[inline]
fn transmute(ctx: &mut u64) -> &mut Context {
    // 这个放在layout的单元测试里面
    //assert_eq!(std::mem::size_of::<Context>(), 8);
    unsafe { std::mem::transmute(ctx) }
}
