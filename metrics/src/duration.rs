// 耗时的metrics，记录了
// 数量，平均耗时，数量，以及不同区间的耗时。
// [0,512us], [512us, 1ms], [1ms, 2ms], [2ms,4ms], [4ms, 8ms], [8ms..]
#[derive(Clone, Debug)]
pub(crate) struct DurationItem {
    pub(crate) count: usize,
    pub(crate) elapse_us: usize,
    pub(crate) intervals: [usize; DURATION_INTERVALS.len()],
}
impl DurationItem {
    pub(crate) fn new() -> Self {
        Self {
            count: 0,
            elapse_us: 0,
            intervals: [0; DURATION_INTERVALS.len()],
        }
    }
    pub(crate) fn get_interval_name(&self, idx: usize) -> &'static str {
        match idx {
            0 => "interval0",
            1 => "interval1",
            2 => "interval2",
            3 => "interval3",
            4 => "interval4",
            5 => "interval5",
            6 => "interval6",
            7 => "interval7",
            8 => "interval8",
            _ => "interval_overflow",
        }
    }
}
use std::ops::AddAssign;

impl AddAssign for DurationItem {
    fn add_assign(&mut self, other: Self) {
        self.count += other.count;
        // 因为这些元素是并发的，所以取耗时时，取最大的即可。而不是直接相加
        self.elapse_us += other.elapse_us;
        for i in 0..self.intervals.len() {
            self.intervals[i] += other.intervals[i];
        }
    }
}
impl AddAssign<Duration> for DurationItem {
    fn add_assign(&mut self, d: Duration) {
        self.count += 1;
        let us = d.as_micros() as usize;
        self.elapse_us += us;
        let idx = get_interval_idx_by_duration_us(us);
        self.intervals[idx] += 1;
    }
}

use std::time::Duration;
impl From<Duration> for DurationItem {
    fn from(d: Duration) -> Self {
        let us = d.as_micros() as usize;
        // 计算us在哪个interval
        let idx = get_interval_idx_by_duration_us(us);
        let mut item = DurationItem::new();
        item.count = 1;
        item.elapse_us = us;
        item.intervals[idx] += 1;
        item
    }
}
// 通过耗时，获取对应的耗时区间，一共分为9个区间
// 左开，右闭区间
#[inline(always)]
pub(crate) fn get_interval_idx_by_duration_us(duration_us: usize) -> usize {
    match DURATION_INTERVALS.binary_search(&duration_us) {
        Ok(idx) => idx,
        Err(idx) => idx,
    }
}

pub(crate) const DURATION_INTERVALS: [usize; 9] = [
    1000 * 4usize.pow(0),
    1000 * 4usize.pow(1),
    1000 * 4usize.pow(2),
    1000 * 4usize.pow(3),
    1000 * 4usize.pow(4),
    1000 * 4usize.pow(5),
    1000 * 4usize.pow(6),
    1000 * 4usize.pow(7),
    std::usize::MAX,
];
