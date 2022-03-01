pub(super) mod config;
pub mod topo;

struct Context {
    runs: u32, // 运行的次数
    idx: u32,
}

#[inline]
fn transmute(ctx: &mut u64) -> &mut Context {
    debug_assert_eq!(std::mem::size_of::<Context>(), 8);
    unsafe { std::mem::transmute(ctx) }
}
