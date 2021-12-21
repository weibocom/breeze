mod data;
mod host;
mod number;
mod qps;
mod rtt;
mod status;

pub(crate) use host::*;
pub(crate) use number::*;
pub(crate) use qps::*;
pub(crate) use rtt::*;
pub(crate) use status::*;

use crate::ItemRc;
pub use data::*;

#[repr(u8)]
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub(crate) enum MetricType {
    Empty = 0u8,
    Qps,
    Status,
    RTT,   // 耗时
    Count, // 计算总的数量，与时间无关。
}

pub(crate) trait Snapshot {
    fn snapshot<W: crate::ItemWriter>(&self, w: &mut W, secs: f64);
}

static METRICS_NAMES: [&'static str; 5] = ["none", "qps", "status", "rtt", "cnt"];

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
            Qps | Status | Count => true,
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
    idx: usize,
    item: ItemRc,
}
impl Metric {
    #[inline(always)]
    pub(crate) fn from(idx: usize) -> Self {
        let mut me = Self {
            idx,
            item: ItemRc::uninit(),
        };
        me.try_inited();
        me
    }
    #[inline(always)]
    fn try_inited(&mut self) {
        self.item.try_init(self.idx);
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
            self.try_inited();
        }
    }
}
use std::fmt::{self, Display, Formatter};
impl Display for Metric {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "name:{:?}", self.idx)
    }
}
unsafe impl Sync for Metric {}
unsafe impl Send for Metric {}
