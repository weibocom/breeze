use crate::Operation;
use ds::RingSlice;

#[derive(Default)]
pub struct Response {
    inner: RingSlice,
    // 包含成功返回的key
    keys: Vec<RingSlice>,
}

static QUIT: &str = "+OK\r\n";

impl Response {
    #[inline]
    pub fn from(data: RingSlice, _op: Operation, keys: Vec<RingSlice>) -> Self {
        Self {
            inner: data,
            keys: keys,
        }
    }
    // TODO：这个后续放在Protocol，暂时加这里for test fishermen
    pub fn with_quit() -> Self {
        let data_slice =
            RingSlice::from(QUIT.as_ptr(), QUIT.len().next_power_of_two(), 0, QUIT.len());
        let keys = vec![data_slice.clone()];
        Response::from(data_slice, Operation::Quit, keys)
    }
    #[inline]
    pub fn keys(&self) -> &[RingSlice] {
        &self.keys
    }
    #[inline]
    pub fn last_key(&self) -> &RingSlice {
        assert!(self.keys.len() > 0);
        unsafe { &self.keys.get_unchecked(self.keys.len() - 1) }
    }
    #[inline]
    pub fn data(&self) -> &RingSlice {
        &self.inner
    }
}
impl AsRef<RingSlice> for Response {
    #[inline]
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
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

use std::fmt::{self, Debug, Display, Formatter};
impl Display for Response {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "key len:{} data:{}", self.keys.len(), self.inner)
    }
}
impl Debug for Response {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "key len:{} data:{}", self.keys.len(), self.inner)
    }
}
