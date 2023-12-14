use super::{base::Adder, IncrTo, ItemData0};
use crate::ItemWriter as Writer;

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub struct StatusData;
impl super::Snapshot for StatusData {
    // 只计数。
    #[inline]
    fn snapshot<W: Writer>(&self, path: &str, key: &str, data: &ItemData0, w: &mut W, _secs: f64) {
        let down = data.d0.take() > 0;
        if down {
            w.write(path, key, "down", 1i64);
        }
    }
}

#[repr(u8)]
#[derive(Clone, Copy)]
pub enum Status {
    OK,
    Down,
}

impl IncrTo for Status {
    #[inline]
    fn incr_to(&self, data: &ItemData0) {
        data.d0.set(*self as i64);
    }
}
