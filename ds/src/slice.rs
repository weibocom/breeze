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
    #[inline(always)]
    pub fn at(&self, pos: usize) -> u8 {
        debug_assert!(pos < self.len());
        unsafe { *(self.ptr as *const u8).offset(pos as isize) }
    }
    #[inline]
    pub fn copy_to_vec(&self, v: &mut Vec<u8>) {
        debug_assert!(self.len() > 0);
        v.reserve(self.len());
        use std::ptr::copy_nonoverlapping as copy;
        unsafe {
            copy(
                self.ptr as *const u8,
                v.as_mut_ptr().offset(v.len() as isize),
                self.len(),
            );
            v.set_len(v.len() + self.len());
        }
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

use std::fmt::{self, Display, Formatter};
impl Display for Slice {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Slice: ptr:{} len:{} ", self.ptr as usize, self.len)
    }
}
