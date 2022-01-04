use std::str::{self, Utf8Error};
// debug only
pub(crate) trait Utf8 {
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
