use super::base::Adder;
use crate::{ItemData, ItemWriter as Writer};
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub struct Qps;
impl super::Snapshot for Qps {
    // 只计数。
    #[inline]
    fn snapshot<W: Writer>(&self, path: &str, key: &str, data: &ItemData, w: &mut W, secs: f64) {
        let num = data.d0.take();
        if num > 0 {
            w.write(path, key, "qps", num as f64 / secs);
        }
    }
}
