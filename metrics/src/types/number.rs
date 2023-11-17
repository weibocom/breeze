use std::sync::atomic::{AtomicI64, Ordering};

use crate::{Id, ItemWriter};

pub(crate) struct NumberInner {
    cur: AtomicI64,
}
impl NumberInner {
    #[inline]
    fn load(&self) -> i64 {
        self.cur.load(Ordering::Relaxed)
    }
    #[inline]
    pub(crate) fn incr(&self, v: i64) {
        self.cur.fetch_add(v, Ordering::Relaxed);
    }
    #[inline]
    pub(crate) fn take(&self) -> i64 {
        let cur = self.load();
        if cur > 0 {
            self.cur.fetch_sub(cur, Ordering::Relaxed);
        }
        cur
    }
    #[inline]
    pub(crate) fn zero(&self) -> i64 {
        let cur = self.load();
        if cur > 0 {
            self.cur.store(0, Ordering::Relaxed);
        }
        cur
    }
    #[inline]
    pub(crate) fn max(&self, v: i64) {
        let cur = self.load();
        if cur < v {
            self.cur.store(v, Ordering::Relaxed);
        }
    }
}

pub struct Number {
    pub(crate) inner: NumberInner,
}
impl Number {
    // 只计数。
    #[inline]
    pub(crate) fn snapshot<W: ItemWriter>(&self, id: &Id, w: &mut W, _secs: f64) {
        let cur = self.inner.cur.load(Ordering::Relaxed);
        if cur > 0 {
            w.write(&id.path, id.key, id.t.name(), cur);
        }
    }
    #[inline]
    pub(crate) fn incr(&self, v: i64) {
        self.inner.incr(v);
    }
    #[inline]
    pub(crate) fn zero(&self) {
        self.inner.zero();
    }
}

pub trait ToNumber {
    fn int(&self) -> i64;
}

macro_rules! impl_to_number {
    ($($t:ty),+) => {
        $(
        impl ToNumber for $t {
            #[inline]
            fn int(&self) -> i64 {
                *self as i64
            }
        }
        )+
    };
}
impl_to_number!(i8, u8, i16, u16, i32, u32, isize, usize, i64, u64);
