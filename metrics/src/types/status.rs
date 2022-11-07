use crate::{Id, ItemWriter, NumberInner};

pub(crate) struct StatusData {
    inner: NumberInner,
}
impl StatusData {
    // 只计数。
    #[inline]
    pub(crate) fn snapshot<W: ItemWriter>(&self, id: &Id, w: &mut W, _secs: f64) {
        let down = self.inner.take() > 0;
        if down {
            w.write(&id.path, id.key, "down", 1i64);
        }
    }
}
