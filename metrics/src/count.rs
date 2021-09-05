pub(crate) struct Count(usize);

impl From<usize> for Count {
    #[inline(always)]
    fn from(v: usize) -> Self {
        Self(v)
    }
}

use std::ops::AddAssign;
impl AddAssign for Count {
    #[inline(always)]
    fn add_assign(&mut self, other: Count) {
        self.0 += other.0
    }
}

impl crate::kv::KvItem for Count {
    #[inline]
    fn with_item<F: Fn(&'static str, f64)>(&self, secs: f64, f: F) {
        // 平均耗时
        f("", self.0 as f64 / secs);
    }
}

impl AddAssign<usize> for Count {
    #[inline(always)]
    fn add_assign(&mut self, other: usize) {
        self.0 += other
    }
}
