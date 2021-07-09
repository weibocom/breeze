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
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr as *const u8
    }
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr as *mut u8
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
