use super::{base::Adder, ItemData0};
use crate::ItemWriter as Writer;

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub struct Status;
impl super::Snapshot for Status {
    // 只计数。
    #[inline]
    fn snapshot<W: Writer>(&self, path: &str, key: &str, data: &ItemData0, w: &mut W, _secs: f64) {
        let down = data.d0.take() > 0;
        if down {
            w.write(path, key, "down", 1i64);
        }
    }
    fn merge(&self, global: &ItemData0, cache: &ItemData0) {
        if global.d0.get() == 0 {
            global.d0.set(cache.d0.take());
        }
    }
}

pub mod pub_status {
    use crate::{base::Adder, IncrTo, ItemData0};
    #[repr(u8)]
    #[derive(Debug, Clone, Copy)]
    pub enum Status {
        Ok = 1,
        Down = 2,
    }

    impl IncrTo for Status {
        #[inline]
        fn incr_to(&self, data: &ItemData0) {
            data.d0.set(*self as i64);
        }
    }
}
