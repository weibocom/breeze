use crate::{Id, ItemWriter, NumberInner};

pub(crate) struct StatusData {
    inner: NumberInner,
}
impl StatusData {
    // 只计数。
    #[inline(always)]
    pub(crate) fn snapshot<W: ItemWriter>(&self, id: &Id, w: &mut W, _secs: f64) {
        let (ss, cur) = self.inner.load_and_snapshot();
        if cur > ss {
            w.write(&id.path, id.key, "down", 1f64);
        }
    }
}
