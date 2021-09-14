pub struct Ratio(usize, usize);

impl From<(usize, usize)> for Ratio {
    #[inline(always)]
    fn from(v: (usize, usize)) -> Self {
        Self(v.0, v.1)
    }
}

use std::ops::AddAssign;
impl AddAssign for Ratio {
    #[inline(always)]
    fn add_assign(&mut self, other: Ratio) {
        self.0 += other.0;
        self.1 += other.1;
    }
}

impl crate::kv::KvItem for Ratio {
    #[inline]
    fn with_item<F: Fn(&'static str, f64)>(&self, _secs: f64, f: F) {
        // 平均耗时
        if self.1 > 0 {
            f("ratio", self.0 as f64 / self.1 as f64);
        }
    }
}

impl AddAssign<(usize, usize)> for Ratio {
    #[inline(always)]
    fn add_assign(&mut self, other: (usize, usize)) {
        self.0 += other.0;
        self.1 += other.1;
    }
}
