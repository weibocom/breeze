use std::str::{self, Utf8Error};
// debug only
pub trait Utf8 {
    // 为了调式。这个地方的可能存在额外的复制。
    fn utf8(&self) -> Result<String, Utf8Error>;
}
impl Utf8 for ds::RingSlice {
    #[inline(always)]
    fn utf8(&self) -> Result<String, Utf8Error> {
        let mut v: Vec<u8> = Vec::with_capacity(self.len());
        self.copy_to_vec(&mut v);
        str::from_utf8(&v).map(|s| s.to_string())
    }
}

impl Utf8 for &[u8] {
    #[inline(always)]
    fn utf8(&self) -> Result<String, Utf8Error> {
        str::from_utf8(self).map(|s| s.to_string())
    }
}
impl Utf8 for Vec<u8> {
    #[inline(always)]
    fn utf8(&self) -> Result<String, Utf8Error> {
        str::from_utf8(self).map(|s| s.to_string())
    }
}
