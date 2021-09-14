pub(crate) struct Qps(usize);

impl From<usize> for Qps {
    #[inline(always)]
    fn from(v: usize) -> Self {
        Self(v)
    }
}

use std::ops::AddAssign;
impl AddAssign for Qps {
    #[inline(always)]
    fn add_assign(&mut self, other: Qps) {
        self.0 += other.0
    }
}

impl crate::kv::KvItem for Qps {
    #[inline]
    fn with_item<F: Fn(&'static str, f64)>(&self, secs: f64, f: F) {
        // 平均耗时
        f("qps", self.0 as f64 / secs);
    }
}

impl AddAssign<usize> for Qps {
    #[inline(always)]
    fn add_assign(&mut self, other: usize) {
        self.0 += other
    }
}
