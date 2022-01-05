mod data;
mod host;
mod number;
mod qps;
mod ratio;
mod rtt;
mod status;

pub(crate) use host::*;
pub(crate) use number::*;
pub(crate) use qps::*;
pub(crate) use ratio::*;
pub(crate) use rtt::*;
pub(crate) use status::*;

use crate::{Id, ItemRc};
pub use data::*;

use std::sync::Arc;

#[repr(u8)]
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub(crate) enum MetricType {
    Empty = 0u8,
    Qps,
    Status,
    RTT,   // 耗时
    Count, // 计算总的数量，与时间无关。
    Ratio,
}

pub(crate) trait Snapshot {
    fn snapshot<W: crate::ItemWriter>(&self, w: &mut W, secs: f64);
}

//static METRICS_NAMES: [&'static str; 5] = ["none", "qps", "status", "rtt", "num"];
static METRICS_NAMES: [&'static str; 6] = ["none", "qps", "status", "rtt", "num", "ratio"];

impl Default for MetricType {
    fn default() -> Self {
        Self::Empty
    }
}

impl MetricType {
    #[inline(always)]
    pub(crate) fn is_empty(&self) -> bool {
        *self as u8 == Self::Empty as u8
    }
    #[inline(always)]
    pub(crate) fn is_num(&self) -> bool {
        use MetricType::*;
        match self {
            Ratio | Qps | Status | Count => true,
            _ => false,
        }
    }
    #[inline(always)]
    pub(crate) fn name(&self) -> &'static str {
        debug_assert!((*self as usize) < METRICS_NAMES.len());
        METRICS_NAMES[*self as usize]
    }
}

pub struct Metric {
    cache: i64, // 未初始化完成前，数据cache
    id: Arc<Id>,
    item: ItemRc,
}
impl Metric {
    #[inline(always)]
    pub(crate) fn from(id: Arc<Id>) -> Self {
        let mut me = Self {
            id,
            item: ItemRc::uninit(),
            cache: 0,
        };
        me.try_inited();
        me
    }
    #[inline(always)]
    fn try_inited(&mut self) {
        self.item.try_init(&self.id);
        if self.cache > 0 && self.item.inited() {
            // flush cache
            self.item.data().flush(self.cache);
            self.cache = 0;
        }
    }
}
use std::fmt::Debug;
use std::ops::AddAssign;
impl<T: MetricData + Debug> AddAssign<T> for Metric {
    #[inline(always)]
    fn add_assign(&mut self, m: T) {
        if self.item.inited() {
            m.incr_to(self.item.data());
        } else {
            m.incr_to_cache(&mut self.cache);
            self.try_inited();
        }
    }
}
use std::ops::SubAssign;
impl<T: MetricData> SubAssign<T> for Metric {
    #[inline(always)]
    fn sub_assign(&mut self, m: T) {
        if self.item.inited() {
            m.decr_to(self.item.data());
        } else {
            m.decr_to_cache(&mut self.cache);
            self.try_inited();
        }
    }
}
use std::fmt::{self, Display, Formatter};
impl Display for Metric {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "name:{:?}", self.id)
    }
}
unsafe impl Sync for Metric {}
unsafe impl Send for Metric {}
