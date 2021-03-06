use std::time::Duration;

use crate::{Id, ItemWriter, NumberInner};
const SLOW: i64 = Duration::from_millis(100).as_micros() as i64;
const MAX: i64 = Duration::from_millis(30).as_micros() as i64;
pub struct Rtt {
    count: NumberInner,
    avg_us: NumberInner,
    slow: NumberInner,
    max: NumberInner,
}

impl Rtt {
    #[inline]
    pub(crate) fn snapshot<W: ItemWriter>(&self, id: &Id, w: &mut W, secs: f64) {
        // qps
        let count = self.count.take();
        if count > 0 {
            w.write(&id.path, id.key, "qps", count as f64 / secs);
            // avg_us
            let avg_us = self.avg_us.take() as f64;
            // 按微秒取整
            let avg = (avg_us / count as f64) as isize as f64;
            w.write(&id.path, id.key, "avg_us", avg);

            // slow qps
            let slow = self.slow.take();
            if slow > 0 {
                w.write(&id.path, id.key, "qps_itvl100ms", slow as f64 / secs);
            }
            let max = self.max.zero();
            if max > 0 {
                w.write(&id.path, id.key, "max_us", max as f64);
            }
        }
    }

    #[inline]
    pub(crate) fn incr(&self, d: Duration) {
        self.count.incr(1);
        let us = d.as_micros() as i64;
        self.avg_us.incr(us);
        if us >= SLOW {
            self.slow.incr(1);
        }
        if us >= MAX {
            self.max.max(us);
        }
    }
}
