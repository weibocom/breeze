use crate::MetricType;
#[derive(Debug, Hash, PartialEq, Eq, Default)]
pub struct Id {
    pub(crate) path: String,
    pub(crate) key: &'static str,
    pub(crate) t: MetricType,
}
impl Id {
    #[inline]
    pub(crate) fn empty(&self) -> bool {
        self.path.len() == 0 && self.t.u8() == 0
    }
}
pub(crate) const BASE_PATH: &str = "base";
