/// 使用者确保Slice持有的数据不会被释放。
pub struct Slice {
    ptr: *const u8,
    len: usize,
}

impl Slice {
    pub fn from(data: &[u8]) -> Self {
        Self {
            ptr: data.as_ptr(),
            len: data.len(),
        }
    }
    #[inline(always)]
    pub fn data(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl AsRef<[u8]> for Slice {
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        self.data()
    }
}

impl Default for Slice {
    fn default() -> Self {
        Slice {
            ptr: 0 as *const u8,
            len: 0,
        }
    }
}
