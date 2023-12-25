use super::{base::Adder, ItemData};
use crate::ItemWriter as Writer;

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub struct Status;
impl super::Snapshot for Status {
    // 只计数。
    #[inline]
    fn snapshot<W: Writer>(&self, path: &str, key: &str, data: &ItemData, w: &mut W, _secs: f64) {
        let v = data.d0.get();

        let down = v != 0;

        if down {
            let v = if v < 0 {
                data.d0.decr_by(v);
                -v
            } else {
                v
            };
            let v = v.abs();
            w.write(path, key, "down", v);
        }
    }
    fn merge(&self, global: &ItemData, cache: &ItemData) {
        if global.d0.get() == 0 {
            global.d0.set(cache.d0.take());
        }
    }
}

pub mod pub_status {
    use crate::{base::Adder, IncrTo, ItemData};
    // 0: ok
    // -1: error
    // 2..: notify
    // 大于0的值，不会在snapshot的时候被take走，会一直保留
    #[derive(Debug, Clone, Copy)]
    pub struct Status(isize);
    impl Status {
        pub const OK: Self = Status(0);
        pub const ERROR: Self = Status(-1);
        pub const NOTIFY: Self = Status(2); // 不会在snapshot的时候被take走，会一直保留
        #[inline(always)]
        pub fn notify(v: usize) -> Self {
            Self(v as isize)
        }
        #[inline(always)]
        pub fn from(ok: bool) -> Self {
            let v = ok as isize;
            // ok => 0
            // !ok => -1
            Self(v - 1)
        }
    }

    impl IncrTo for Status {
        #[inline]
        fn incr_to(&self, data: &ItemData) {
            data.d0.set(self.0 as i64);
        }
    }
}
