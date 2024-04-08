pub(crate) const TARGET_SPLIT: char = '/';
#[derive(Hash, PartialEq, Eq, Default)]
pub struct Id {
    pub(crate) path: String,
    pub(crate) key: &'static str,
    pub(crate) t: MetricType,
}
// 为Id实现Debug
impl std::fmt::Debug for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{:?}", self.path, self.key, self.t)
    }
}
pub(crate) const BASE_PATH: &str = "base";

use crate::Metric;
#[derive(Debug, Clone)]
pub struct Path {
    path: String,
}
impl Path {
    #[inline]
    pub fn base() -> Self {
        Self::new(vec![crate::BASE_PATH])
    }
    #[inline]
    pub fn pop(&self) -> Self {
        let mut new = self.clone();
        let len = new.path.rfind(TARGET_SPLIT).unwrap_or(0);
        new.path.truncate(len);
        new
    }
    #[inline]
    pub fn new<T: AsRef<str>>(names: Vec<T>) -> Self {
        let mut me = Self {
            path: String::with_capacity(128),
        };
        names.iter().for_each(|x| me.push(x));
        me
    }
    fn with_type(&self, key: &'static str, t: MetricType) -> Metric {
        let path = self.path.clone();
        let id = Id { path, key, t };
        crate::register_metric(id)
    }
    pub fn push<T: AsRef<str>>(&mut self, name: T) {
        if self.path.len() > 0 {
            self.path.push(TARGET_SPLIT);
        }
        self.path.push_str(name.as_ref());
    }
    pub fn num(&self, key: &'static str) -> Metric {
        self.with_type(key, MetricType::Count(Count))
    }
    pub fn rtt(&self, key: &'static str) -> Metric {
        self.with_type(key, MetricType::Rtt(Rtt))
    }
    pub fn status(&self, key: &'static str) -> Metric {
        self.with_type(key, MetricType::Status(Status))
    }
    pub fn ratio(&self, key: &'static str) -> Metric {
        self.with_type(key, MetricType::Ratio(Ratio))
    }
    pub fn qps(&self, key: &'static str) -> Metric {
        self.with_type(key, MetricType::Qps(Qps))
    }
}

use crate::{types::Status, Count, Empty, Qps, Ratio, Rtt};
use enum_dispatch::enum_dispatch;
#[enum_dispatch(Snapshot)]
#[repr(u8)]
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub(crate) enum MetricType {
    Empty,
    Qps,
    Ratio,
    Status,
    Rtt,
    Count,
}

impl Default for MetricType {
    fn default() -> Self {
        Self::Empty(Empty)
    }
}
