use crate::{Id, ItemWriter, NumberInner};

pub struct Qps {
    inner: NumberInner,
}
impl Qps {
    // 只计数。
    #[inline(always)]
    pub(crate) fn snapshot<W: ItemWriter>(&self, id: &Id, w: &mut W, secs: f64) {
        let (last, cur) = self.inner.load_and_snapshot();
        w.write(&id.path, id.key, id.t.name(), (cur - last) as f64 / secs);
    }
}
