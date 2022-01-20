mod data;
mod host;
mod number;
mod qps;
mod rtt;
mod status;
//pub(crate) mod tasks;

pub(crate) use host::*;
pub use host::{decr_task, incr_task};
pub(crate) use number::*;
pub(crate) use qps::*;
pub(crate) use rtt::*;
pub(crate) use status::*;
//pub use tasks::TASK_NUM;

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
}

pub(crate) trait Snapshot {
    fn snapshot<W: crate::ItemWriter>(&self, w: &mut W, secs: f64);
}

static METRICS_NAMES: [&'static str; 5] = ["none", "qps", "status", "rtt", "num"];

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
    id: Arc<Id>,
    item: ItemRc,
}
impl Metric {
    #[inline(always)]
    pub(crate) fn from(id: Arc<Id>) -> Self {
        let mut me = Self {
            id,
            item: ItemRc::uninit(),
        };
        me.try_inited();
        me
    }
    #[inline(always)]
    fn try_inited(&mut self) {
        self.item.try_init(&self.id);
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
            m.incr_to_cache(&self.id);
            self.try_inited();
        }
    }
}
use std::ops::SubAssign;
impl<T: MetricData + std::ops::Neg<Output = T> + Debug> SubAssign<T> for Metric {
    #[inline(always)]
    fn sub_assign(&mut self, m: T) {
        *self += -m;
    }
}
use std::fmt::{self, Display, Formatter};
impl Display for Metric {
    #[inline(always)]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "name:{:?}", self.id.path)
    }
}
impl Debug for Metric {
    #[inline(always)]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "name:{:?}", self.id)
    }
}
unsafe impl Sync for Metric {}
unsafe impl Send for Metric {}
