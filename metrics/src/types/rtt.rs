use ds::time::Duration;

use super::{base::Adder, IncrTo, ItemData};
use crate::ItemWriter as Writer;
pub const MAX: Duration = Duration::from_millis(30);
const SLOW_US: i64 = Duration::from_millis(100).as_micros() as i64;
const MAX_US: i64 = MAX.as_micros() as i64;

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub struct Rtt;
//{
//    count: NumberInner,
//    total_us: NumberInner,
//    slow: NumberInner,
//    max: NumberInner,
//}

// d0: 总的数量
// d1: 总的耗时
// d2: 慢的数量
// d3: 最大的耗时
impl super::Snapshot for Rtt {
    #[inline]
    fn snapshot<W: Writer>(&self, path: &str, key: &str, data: &ItemData, w: &mut W, secs: f64) {
        // qps
        let count = data.d0.take();
        if count > 0 {
            w.write(path, key, "qps", count as f64 / secs);
            // avg_us
            let total_us = data.d1.take();
            // 按微秒取整
            let avg = (total_us as f64 / count as f64) as i64;
            w.write(path, key, "avg_us", avg);

            // slow qps
            let slow = data.d2.take();
            if slow > 0 {
                w.write(path, key, "qps_itvl100ms", slow as f64 / secs);
            }
            let max = data.d3.take();
            if max > 0 {
                w.write(path, key, "max_us", max);
            }
        }
    }
}

// d0: 总的数量
// d1: 总的耗时
// d2: 慢的数量
// d3: 最大的耗时
impl IncrTo for Duration {
    #[inline]
    fn incr_to(&self, data: &ItemData) {
        // 总的数量
        data.d0.incr();
        let us = self.as_micros() as i64;
        // 总的耗时
        data.d1.incr_by(us);
        if self.as_micros() as i64 >= SLOW_US {
            // 慢的数量
            data.d2.incr();
        }
        if us >= MAX_US {
            data.d3.max(us);
        }
    }
}
