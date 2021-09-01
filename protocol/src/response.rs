use crate::Operation;
use ds::RingSlice;

#[derive(Default)]
pub struct Response {
    _op: Operation,
    inner: RingSlice,
    keys: Vec<RingSlice>,
}

impl Response {
    #[inline]
    pub fn from(data: RingSlice, op: Operation, keys: Vec<RingSlice>) -> Self {
        Self {
            _op: op,
            inner: data,
            keys: keys,
        }
    }
    #[inline]
    pub fn keys(&self) -> &[RingSlice] {
        &self.keys
    }
}
impl AsRef<RingSlice> for Response {
    #[inline(always)]
    fn as_ref(&self) -> &RingSlice {
        &self.inner
    }
}
use std::ops::{Deref, DerefMut};
impl Deref for Response {
    type Target = RingSlice;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
impl DerefMut for Response {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
