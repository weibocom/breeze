#[derive(Clone, Debug)]
pub(crate) struct Count(isize);

impl From<isize> for Count {
    #[inline(always)]
    fn from(v: isize) -> Self {
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
    #[inline(always)]
    fn with_item<F: Fn(&'static str, f64)>(&self, _secs: f64, f: F) {
        // 平均耗时
        f("num", self.0 as f64);
    }
    // 统计历史的数据，不需要每次都清理
    #[inline]
    fn clear() -> bool {
        false
    }
}

impl AddAssign<isize> for Count {
    #[inline(always)]
    fn add_assign(&mut self, other: isize) {
        self.0 += other
    }
}
