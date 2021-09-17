// 描述状态。比如宕机、在线等.

#[derive(Clone, Debug)]
pub enum Status {
    Init,
    Down,
    Timeout,
}
use std::ops::AddAssign;
impl AddAssign for Status {
    #[inline(always)]
    fn add_assign(&mut self, other: Status) {
        *self = other
    }
}

impl crate::kv::KvItem for Status {
    #[inline]
    fn with_item<F: Fn(&'static str, f64)>(&self, _secs: f64, f: F) {
        match self {
            Self::Down => f("down", 1 as f64),
            Self::Timeout => f("timeout", 1 as f64),
            Self::Init => {}
        }
    }
}
