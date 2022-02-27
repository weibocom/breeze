use crate::{Id, ItemWriter, NumberInner};

pub struct Qps {
    inner: NumberInner,
}
impl Qps {
    // 只计数。
    #[inline]
    pub(crate) fn snapshot<W: ItemWriter>(&self, id: &Id, w: &mut W, secs: f64) {
        let num = self.inner.take();
        if num > 0 {
            w.write(&id.path, id.key, id.t.name(), num as f64 / secs);
        }
    }
}
