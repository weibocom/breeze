use crate::{Id, ItemWriter, NumberInner};

pub struct Ratio {
    hit: NumberInner,
    total: NumberInner,
}
impl Ratio {
    #[inline]
    pub(crate) fn snapshot<W: ItemWriter>(&self, id: &Id, w: &mut W, _secs: f64) {
        let hit = self.hit.take();
        let total = self.total.take();
        if total > 0 {
            const PREC: i64 = 10000;
            let ratio = hit as f64 / total as f64;
            // 保留2位小数精度
            let ratio = ((ratio * PREC as f64) as i64 as f64) / PREC as f64;
            w.write(&id.path, id.key, id.t.name(), ratio);
        }
    }
    #[inline]
    pub(crate) fn incr(&self, hit: bool) {
        self.hit.incr(hit as i64);
        self.total.incr(1);
    }
}
