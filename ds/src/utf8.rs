// debug only
pub trait Utf8 {
    // 为了调式。这个地方的可能存在额外的复制。
    fn utf8(&self) -> String;
}
impl Utf8 for &[u8] {
    #[inline]
    fn utf8(&self) -> String {
        String::from_utf8(
            self.iter()
                .map(|b| std::ascii::escape_default(*b))
                .flatten()
                .collect(),
        )
        .unwrap()
            + &format!(" u8_format:{:?}", self)
    }
}
impl Utf8 for Vec<u8> {
    #[inline]
    fn utf8(&self) -> String {
        self.as_slice().utf8()
    }
}
