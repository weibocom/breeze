/// 使用者确保Slice持有的数据不会被释放。
#[derive(Clone, Debug)]
pub struct Slice {
    ptr: usize,
    len: usize,
}

impl Slice {
    #[inline(always)]
    pub fn new(ptr: usize, len: usize) -> Self {
        Self { ptr, len }
    }
    #[inline(always)]
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
    pub fn data_with_pos(&self, pos: usize) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts((self.ptr as *const u8).offset(pos as isize), self.len - pos)
        }
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
        use crate::Buffer;
        v.write(self);
    }
    #[inline]
    pub fn sub_slice(&self, offset: usize, len: usize) -> Self {
        debug_assert!(offset + len <= self.len);
        Self::new(self.ptr + offset, len)
    }

    pub fn split(&self, splitter: &[u8]) -> Vec<Self> {
        let mut pos = 0 as usize;
        let mut result: Vec<Slice> = vec![];
        loop {
            let new_pos = self.find_sub(pos, splitter);
            if new_pos.is_none() {
                if pos < self.len() {
                    result.push(self.sub_slice(pos, self.len() - pos));
                }
                return result;
            } else {
                let new_pos = new_pos.unwrap();
                result.push(self.sub_slice(pos, new_pos));
                if new_pos + splitter.len() == self.len {
                    return result;
                }
                pos = pos + new_pos + splitter.len();
            }
        }
    }
    fn find_sub(&self, pos: usize, needle: &[u8]) -> Option<usize> {
        self.data_with_pos(pos)
            .windows(needle.len())
            .position(|window| window == needle)
    }

    pub fn update_u8(&self, pos: usize, val: u8) -> bool {
        if pos >= self.len {
            return false;
        }
        let data = self.ptr as *mut u8;
        let d = unsafe { data.offset(pos as isize) };
        unsafe { *d = val };
        return true;
    }

    pub fn update(&self, pos: usize, val: &[u8]) -> bool {
        if pos >= self.len || val.len() > (self.len - pos) {
            log::error!("slice update false for val is too long");
            return false;
        }
        let data = self.ptr as *mut u8;
        let mut idx = pos as isize;
        for v in val {
            let d = unsafe { data.offset(idx) };
            unsafe {
                *d = *v;
            }
            idx += 1;
        }
        return true;
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
use std::hash::{Hash, Hasher};
impl Hash for Slice {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.data().hash(state);
    }
}

use std::convert::TryInto;
use std::slice::from_raw_parts;
macro_rules! define_read_number {
    ($fn_name:ident, $type_name:tt) => {
        #[inline(always)]
        pub fn $fn_name(&self, offset: usize) -> $type_name {
            const SIZE: usize = std::mem::size_of::<$type_name>();
            debug_assert!(self.len() >= offset + SIZE);
            unsafe {
                let b = from_raw_parts((self.ptr as *const u8).offset(offset as isize), SIZE);
                $type_name::from_be_bytes(b[..SIZE].try_into().unwrap())
            }
        }
    };
}

impl Slice {
    // big endian
    define_read_number!(read_u16, u16);
    define_read_number!(read_u32, u32);
    define_read_number!(read_u64, u64);
}
