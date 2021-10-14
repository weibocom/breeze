// 耗时的metrics，记录了
// 数量，平均耗时，数量，以及不同区间的耗时。
// [0,512us], [512us, 1ms], [1ms, 2ms], [2ms,4ms], [4ms, 8ms], [8ms..]
#[derive(Clone, Debug)]
pub(crate) struct DurationItem {
    pub(crate) count: usize,
    pub(crate) elapse_us: usize,
    pub(crate) intervals: [usize; DURATION_INTERVALS.len()],
}
impl Default for DurationItem {
    fn default() -> Self {
        Self::new()
    }
}
impl DurationItem {
    pub(crate) fn new() -> Self {
        Self {
            count: 0,
            elapse_us: 0,
            intervals: [0; DURATION_INTERVALS.len()],
        }
    }
}
use std::ops::AddAssign;

impl AddAssign for DurationItem {
    #[inline(always)]
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
    #[inline(always)]
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
    #[inline]
    fn from(d: Duration) -> Self {
        let us = d.as_micros() as usize;
        let mut item = DurationItem::new();
        item.count = 1;
        item.elapse_us = us;
        // 计算us在哪个interval
        let idx = get_interval_idx_by_duration_us(us);
        item.intervals[idx] += 1;
        item
    }
}

impl crate::kv::KvItem for DurationItem {
    fn with_item<F: Fn(&'static str, f64)>(&self, secs: f64, f: F) {
        // 平均耗时
        let avg_us = if self.count == 0 {
            0f64
        } else {
            self.elapse_us as f64 / self.count as f64
        };
        f("avg_us", avg_us);
        // 总的qps
        f("qps", self.count as f64 / secs);
        for i in 0..self.intervals.len() {
            let count = self.intervals[i];
            if count > 0 {
                let sub_key = get_interval_name(i);
                let interval_qps = count as f64 / secs;
                f(sub_key, interval_qps);
            }
        }
    }
}
// 通过耗时，获取对应的耗时区间，一共分为9个区间
// 左开，右闭区间
#[inline(always)]
pub(crate) fn get_interval_idx_by_duration_us(duration_us: usize) -> usize {
    for (i, us) in (0..DURATION_INTERVALS.len()).enumerate() {
        if duration_us <= us {
            return i;
        }
    }
    DURATION_INTERVALS.len() - 1
}
const INTERVAL_SIZE: usize = 6;
pub(crate) const DURATION_INTERVALS: [usize; INTERVAL_SIZE] = [
    Duration::from_millis(4).as_micros() as usize,
    Duration::from_millis(64).as_micros() as usize,
    Duration::from_millis(256).as_micros() as usize,
    Duration::from_secs(1).as_micros() as usize,
    Duration::from_secs(4).as_micros() as usize,
    std::usize::MAX,
];
const NAMES: [&'static str; INTERVAL_SIZE] = [
    "itvl0-4ms",
    "itvl4-64ms",
    "itvl64-256ms",
    "itvl256ms-1s",
    "itvl1s-4s",
    "itvl4s+",
];
#[inline(always)]
fn get_interval_name(idx: usize) -> &'static str {
    debug_assert!(idx < NAMES.len());
    NAMES[idx]
}
