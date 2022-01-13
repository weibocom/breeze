use std::sync::atomic::{AtomicI64, Ordering};

use crate::{Id, ItemWriter};

pub(crate) struct NumberInner {
    cur: AtomicI64,
    ss: AtomicI64,
}
impl NumberInner {
    #[inline(always)]
    pub(crate) fn incr(&self, v: i64) {
        self.cur.fetch_add(v, Ordering::Relaxed);
    }
    #[inline(always)]
    pub(crate) fn load_and_snapshot(&self) -> (i64, i64) {
        let ss = self.ss.load(Ordering::Relaxed);
        let cur = self.cur.load(Ordering::Relaxed);
        self.ss.store(cur, Ordering::Relaxed);
        (ss, cur)
    }
}

pub struct Number {
    pub(crate) inner: NumberInner,
}
impl Number {
    // 只计数。
    #[inline(always)]
    pub(crate) fn snapshot<W: ItemWriter>(&self, id: &Id, w: &mut W, _secs: f64) {
        let cur = self.inner.cur.load(Ordering::Relaxed);
        w.write(&id.path, id.key, id.t.name(), cur as f64);
    }
    #[inline(always)]
    pub(crate) fn incr(&self, v: i64) {
        self.inner.incr(v);
    }
}

pub trait ToNumber {
    fn int(&self) -> i64;
}

macro_rules! impl_to_number {
    ($($t:ty),+) => {
        $(
        impl ToNumber for $t {
            #[inline(always)]
            fn int(&self) -> i64 {
                *self as i64
            }
        }
        )+
    };
}
impl_to_number!(i8, u8, i16, u16, i32, u32, isize, usize, i64, u64);
