use std::sync::atomic::{AtomicIsize, Ordering};
pub static TASK_NUM: AtomicIsize = AtomicIsize::new(0);
pub(crate) fn snapshot<W: crate::ItemWriter>(w: &mut W, _secs: f64) {
    let num = TASK_NUM.load(Ordering::Relaxed);
    w.write("mesh", "task", "num", num as f64);
}
