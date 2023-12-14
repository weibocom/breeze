use crate::{Id, ItemData0, Snapshot};
use std::sync::Arc;

#[derive(Default, Debug)]
pub struct ItemData {
    id: Arc<Id>,
    pub(crate) inner: ItemData0,
}

impl ItemData {
    #[inline]
    pub(crate) fn init_id(&mut self, id: Arc<Id>) {
        assert!(!self.id.valid());
        self.id = id;
    }
    #[inline]
    pub(crate) fn snapshot<W: crate::ItemWriter>(&self, w: &mut W, secs: f64) {
        self.id
            .t
            .snapshot(&self.id.path, &self.id.key, &self.inner, w, secs);
    }
}

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
    pub fn num(self, key: &'static str) -> Metric {
        self.with_type(key, MetricType::Count(Count))
    }
}

use crate::{Count, Empty, Qps, Ratio, Rtt, StatusData};
use enum_dispatch::enum_dispatch;
#[enum_dispatch(Snapshot)]
#[repr(u8)]
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub(crate) enum MetricType {
    Empty(Empty),
    Qps(Qps),
    Ratio(Ratio),
    Status(StatusData),
    Rtt(Rtt),
    Count(Count),
}
impl MetricType {
    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        matches!(self, Self::Empty(_))
    }
}

impl Default for MetricType {
    fn default() -> Self {
        Self::Empty(Empty)
    }
}
