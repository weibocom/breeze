// 耗时的metrics，记录了
// 数量，平均耗时，数量，以及不同区间的耗时。
// [0,512us], [512us, 1ms], [1ms, 2ms], [2ms,4ms], [4ms, 8ms], [8ms..]
#[derive(Clone, Debug)]
pub(crate) struct DurationItem {
    pub(crate) count: usize,
    pub(crate) elapse_us: usize,
    pub(crate) slow: usize, // 慢请求数量
                            //pub(crate) intervals: [usize; DURATION_INTERVALS.len()],
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
            slow: 0,
            //intervals: [0; DURATION_INTERVALS.len()],
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
        self.slow += other.slow;
        //for i in 0..self.intervals.len() {
        //    self.intervals[i] += other.intervals[i];
        //}
    }
}
const SLOW: Duration = Duration::from_millis(200);
impl AddAssign<Duration> for DurationItem {
    #[inline(always)]
    fn add_assign(&mut self, d: Duration) {
        self.count += 1;
        let us = d.as_micros() as usize;
        self.elapse_us += us;
        //let idx = get_interval_idx_by_duration_us(us);
        //self.intervals[idx] += 1;
        if d >= SLOW {
            self.slow += 1;
        }
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
        //let idx = get_interval_idx_by_duration_us(us);
        //item.intervals[idx] += 1;
        if d >= SLOW {
            item.slow = 1;
        }
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
        f("slow_qps", self.slow as f64 / secs);
        //for i in 0..self.intervals.len() {
        //    let count = self.intervals[i];
        //    if count > 0 {
        //        let sub_key = self.get_interval_name(i);
        //        let interval_qps = count as f64 / secs;
        //        f(sub_key, interval_qps);
        //    }
        //}
    }
}
// 通过耗时，获取对应的耗时区间，一共分为9个区间
// 左开，右闭区间
//#[inline(always)]
//pub(crate) fn get_interval_idx_by_duration_us(duration_us: usize) -> usize {
//    match DURATION_INTERVALS.binary_search(&duration_us) {
//        Ok(idx) => idx,
//        Err(idx) => idx,
//    }
//}
//
//pub(crate) const DURATION_INTERVALS: [usize; 9] = [
//    1000 * 4usize.pow(0),
//    1000 * 4usize.pow(1),
//    1000 * 4usize.pow(2),
//    1000 * 4usize.pow(3),
//    1000 * 4usize.pow(4),
//    1000 * 4usize.pow(5),
//    1000 * 4usize.pow(6),
//    1000 * 4usize.pow(7),
//    std::usize::MAX,
//];
//pub(crate) fn get_interval_name(&self, idx: usize) -> &'static str {
//    match idx {
//        0 => "itvl0-1ms",
//        1 => "itvl1-4ms",
//        2 => "itvl4-16ms",
//        3 => "itvl16-64ms",
//        4 => "itvl64-256ms",
//        5 => "itvl256ms-1s",
//        6 => "itvl1s-4s",
//        7 => "itvl4s-16s",
//        8 => "itvl16s-",
//        _ => "itvl_overflow",
//    }
//}
