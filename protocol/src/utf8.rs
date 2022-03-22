use std::str;
// debug only
pub trait Utf8 {
    // 为了调式。这个地方的可能存在额外的复制。
    fn utf8(&self) -> Result<String, Vec<u8>>;
}
impl Utf8 for ds::RingSlice {
    #[inline]
    fn utf8(&self) -> Result<String, Vec<u8>> {
        let mut v: Vec<u8> = Vec::with_capacity(self.len());
        self.copy_to_vec(&mut v);
        str::from_utf8(&v).map(|s| s.to_string()).map_err(|_| v)
    }
}

impl Utf8 for &[u8] {
    #[inline]
    fn utf8(&self) -> Result<String, Vec<u8>> {
        str::from_utf8(self)
            .map(|s| s.to_string())
            .map_err(|_| Vec::from(*self))
    }
}
impl Utf8 for Vec<u8> {
    #[inline]
    fn utf8(&self) -> Result<String, Vec<u8>> {
        str::from_utf8(self)
            .map(|s| s.to_string())
            .map_err(|_| self.clone())
    }
}
