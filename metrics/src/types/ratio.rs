use crate::{Id, ItemWriter, NumberInner};

pub struct Ratio {
    hit: NumberInner,
    total: NumberInner,
}
impl Ratio {
    #[inline(always)]
    pub(crate) fn snapshot<W: ItemWriter>(&self, id: &Id, w: &mut W, _secs: f64) {
        let (hit_last, hit_cur) = self.hit.load_and_snapshot();
        let (total_last, total_cur) = self.total.load_and_snapshot();
        if total_cur > total_last {
            const PREC: i64 = 10000;
            let ratio = (hit_cur - hit_last) as f64 / (total_cur - total_last) as f64;
            // 保留2位小数精度
            let ratio = ((ratio * PREC as f64) as i64 as f64) / PREC as f64;
            w.write(&id.path, id.key, id.t.name(), ratio);
        }
    }
    #[inline(always)]
    pub(crate) fn incr<T: crate::ToNumber>(&self, ratio: (T, T)) {
        self.hit.incr(ratio.0.int());
        self.total.incr(ratio.1.int());
    }
}
