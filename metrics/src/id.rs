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

//#[derive(Default, Debug)]
//pub struct ItemData {
//    pub(crate) inner: ItemData,
//}

use crate::Metric;
#[derive(Debug, Clone)]
pub struct Path {
    path: Vec<String>,
}
impl Path {
    #[inline]
    pub fn base() -> Self {
        Self::new(vec![crate::BASE_PATH])
    }
    #[inline]
    pub fn pop(&self) -> Self {
        let mut new = self.clone();
        new.path.pop();
        new
    }
    #[inline]
    pub fn new<T: ToString>(names: Vec<T>) -> Self {
        Self {
            path: names.into_iter().map(|s| s.to_string()).collect(),
        }
    }
    fn with_type(&self, key: &'static str, t: MetricType) -> Metric {
        let mut s: String = String::with_capacity(256);
        for name in self.path.iter() {
            s += &crate::encode_addr(name.as_ref());
            s.push(crate::TARGET_SPLIT as char);
        }
        s.pop();
        s.shrink_to_fit();
        let id = Id { path: s, key, t };
        crate::register_metric(id)
    }
    pub fn push(mut self, name: &str) -> Self {
        self.path.push(name.to_string());
        self
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
