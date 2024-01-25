use super::{base::Adder, Snapshot};
use crate::{IncrTo, ItemData, ItemWriter as Writer};

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub struct Ratio;

impl Snapshot for Ratio {
    #[inline]
    fn snapshot<W: Writer>(&self, path: &str, key: &str, data: &ItemData, w: &mut W, _secs: f64) {
        let hit = data.d0.take();
        let total = data.d1.take();
        if total > 0 {
            const PREC: i64 = 10000;
            let ratio = hit as f64 / total as f64;
            // 保留2位小数精度
            let ratio = ((ratio * PREC as f64) as i64 as f64) / PREC as f64;
            w.write(path, key, "ratio", ratio);
        }
    }
}

// 对于bool类型，用来统计命中率。
// d0: 命中的数量
// d1: 总的数量
impl IncrTo for bool {
    #[inline]
    fn incr_to(&self, data: &ItemData) {
        data.d0.incr_by(*self as i64);
        data.d1.incr();
    }
}
