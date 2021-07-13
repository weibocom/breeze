/// 使用者确保Slice持有的数据不会被释放。
#[derive(Clone)]
pub struct Slice {
    ptr: usize,
    len: usize,
}

impl Slice {
    pub fn new(ptr: usize, len: usize) -> Self {
        Self { ptr, len }
    }
    pub fn from(data: &[u8]) -> Self {
        Self {
            ptr: data.as_ptr() as usize,
            len: data.len(),
        }
    }
    #[inline(always)]
    pub fn data(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr as *const u8, self.len) }
    }
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len
    }
    #[inline(always)]
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr as *const u8
    }
    #[inline(always)]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr as *mut u8
    }
    #[inline(always)]
    pub fn backwards(&mut self, n: usize) {
        debug_assert!(self.len >= n);
        self.len -= n;
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
        Slice { ptr: 0, len: 0 }
    }
}
use std::ops::Deref;
impl Deref for Slice {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.data()
    }
}
