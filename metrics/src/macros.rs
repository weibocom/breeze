use crate::{Id, ItemWriter};
use std::sync::Arc;

pub trait MetricData: Sized {
    fn incr_to(self, data: &ItemData);
    fn incr_to_cache(self, _id: &Arc<Id>) {}
    fn zero(self, _data: &ItemData) {}
}

#[derive(Default, Debug)]
pub struct ItemData {
    id: Arc<Id>,
    inner: InnerData,
}

impl ItemData {
    #[inline]
    pub(crate) fn init_id(&mut self, id: Arc<Id>) {
        assert!(!self.id.valid());
        self.id = id;
    }
    #[inline]
    pub(crate) fn flush(&self, cache: i64) {
        if self.id.t.need_flush() {
            unsafe { self.incr_num(cache) };
        }
    }

    #[inline]
    unsafe fn incr_num(&self, num: i64) {
        assert!(self.id.t.is_num());
        self.inner.num.incr(num);
    }
    #[inline]
    pub fn zero_num(&self) {
        assert!(self.id.t.is_num());
        unsafe {
            self.inner.num.zero();
        }
    }
}
use crate::ToNumber;
impl<T: ToNumber> MetricData for T {
    #[inline]
    fn incr_to(self, data: &ItemData) {
        unsafe { data.incr_num(self.int()) };
    }
    #[inline]
    fn incr_to_cache(self, id: &Arc<Id>) {
        crate::register_cache(id, self.int());
    }
    #[inline]
    fn zero(self, data: &ItemData) {
        data.zero_num();
    }
}
use ds::time::Duration;
impl MetricData for Duration {
    #[inline]
    fn incr_to(self, data: &ItemData) {
        unsafe { data.inner.rtt.incr(self) };
    }
}
impl MetricData for bool {
    #[inline]
    fn incr_to(self, data: &ItemData) {
        unsafe { data.inner.ratio.incr(self) };
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
}

macro_rules! define_metrics {
    ($($var:ident, $name:ident, $ty:ty, $is_num:expr, $need_flush:expr);+) => {
#[repr(u8)]
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub(crate) enum MetricType {
    Empty = 0u8,
    $($var),+
}

impl MetricType {
    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        *self as u8 == Self::Empty as u8
    }
    #[inline]
    pub(crate) fn is_num(&self) -> bool {
        static IS_NUMS:[bool; MetricType::var_num()] = [false, $($is_num),+];
        assert!((*self as usize) < IS_NUMS.len());
        IS_NUMS[*self as usize]
    }
    #[inline]
    pub(crate) fn need_flush(&self) -> bool {
        static NEED_FLUSH:[bool; MetricType::var_num()] = [false, $($need_flush),+];
        assert!((*self as usize) < NEED_FLUSH.len());
        NEED_FLUSH[*self as usize]
    }
    #[inline]
    pub(crate) fn name(&self) -> &'static str{
        static METRICS_NAMES:[&'static str; MetricType::var_num()] = ["none", $(stringify!($name)),+];
        assert!((*self as usize) < METRICS_NAMES.len());
        METRICS_NAMES[*self as usize]
    }
    #[inline]
    const fn var_num() -> usize {
        const NUM:usize = 1 $( + (stringify!($name).len() > 0) as usize)+;
        NUM
    }
}


impl Default for MetricType {
    fn default() -> Self {
        Self::Empty
    }
}


use std::mem::ManuallyDrop;
// InnerData存储在Item里面，每一个chunk的生命周期都是static的。
union InnerData {
    empty: [u8; 48], // CacheLineSize. 一个Item是一个CacheLine
    $(
        $name: ManuallyDrop<$ty>,
    )+
}

impl Default for InnerData {
    #[inline]
    fn default() -> Self {
        Self { empty: [0u8; 48] }
    }
}

impl std::fmt::Debug for InnerData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "inner data(*) ")
    }
}

impl ItemData {
    #[inline]
    pub(crate) fn snapshot<W: ItemWriter>(&self, w: &mut W, secs: f64) {
        use MetricType::*;
        unsafe {
            match self.id.t {
                Empty => panic!("metric type empty, not inited"),
                $(
                    $var => self.inner.$name.snapshot(&*self.id, w, secs),
                )+
            }
        }
    }
}

impl Path {
    $(
    #[inline]
    pub fn $name(&self, key: &'static str) -> crate::Metric {
        self.with_type(key, crate::MetricType::$var)
    }
    )+
}


    };
} // end with define_metrics

define_metrics!(
//enum var, fieldname, type                is number,    support flushing(只有count类型支持)
    Qps,    qps,       crate::Qps,         true,         false;
    Ratio,  ratio,     crate::Ratio,       false,        false;
    Status, status,    crate::StatusData,  true,         false;
    Rtt,    rtt,       crate::Rtt,         false,        false;
    Count,  num,       crate::Number,      true,         true
);
