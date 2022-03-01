use std::sync::atomic::{AtomicUsize, Ordering};
static SEQ: AtomicUsize = AtomicUsize::new(0);

#[inline]
pub fn next_seq() -> usize {
    SEQ.fetch_add(1, Ordering::Relaxed)
}
