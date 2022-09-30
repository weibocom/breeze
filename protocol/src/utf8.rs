use std::str;
// debug only
pub trait Utf8 {
    // 为了调式。这个地方的可能存在额外的复制。
    fn utf8(&self) -> String;
}
impl Utf8 for ds::RingSlice {
    #[inline]
    fn utf8(&self) -> String {
        let mut v: Vec<u8> = Vec::with_capacity(self.len());
        self.copy_to_vec(&mut v);
        v.utf8()
    }
}

impl Utf8 for &[u8] {
    #[inline]
    fn utf8(&self) -> String {
        str::from_utf8(self)
            .map(|s| s.to_string())
            .unwrap_or_else(|_| format!("{:?}", self))
    }
}
impl Utf8 for Vec<u8> {
    #[inline]
    fn utf8(&self) -> String {
        self.as_slice().utf8()
    }
}
