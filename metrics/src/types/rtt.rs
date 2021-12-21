use std::time::Duration;

use crate::{Id, ItemWriter, NumberInner};
const SLOW: i64 = Duration::from_millis(200).as_micros() as i64;
pub struct Rtt {
    count: NumberInner,
    avg_us: NumberInner,
    slow: NumberInner,
}

impl Rtt {
    #[inline(always)]
    pub(crate) fn snapshot<W: ItemWriter>(&self, id: &Id, w: &mut W, secs: f64) {
        // qps
        let (ss, cur) = self.count.load_and_snapshot();
        let count = cur - ss;
        w.write(&id.path, id.key, "qps", count as f64 / secs);

        if count > 0 {
            // avg_us
            let (ss, cur) = self.avg_us.load_and_snapshot();
            w.write(
                &id.path,
                id.key,
                "avg_us",
                (cur - ss) as f64 / count as f64 / secs,
            );

            // slow qps
            let (ss, cur) = self.slow.load_and_snapshot();
            let slow = (cur - ss) as f64;
            w.write(&id.path, id.key, "qps.itvl200ms-MAX", slow / secs);
        }
    }

    #[inline(always)]
    pub(crate) fn incr(&self, d: Duration) {
        self.count.incr(1);
        let us = d.as_micros() as i64;
        self.avg_us.incr(us);
        if us >= SLOW {
            self.slow.incr(1);
        }
    }
}
