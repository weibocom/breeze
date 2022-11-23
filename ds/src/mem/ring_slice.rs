use std::fmt::{Debug, Display, Formatter};
use std::io::{Error, ErrorKind, Result};
use std::ptr::copy_nonoverlapping;
use std::slice::from_raw_parts;

#[derive(Default)]
pub struct RingSlice {
    ptr: usize,
    cap: usize,
    start: usize,
    len: usize,
}

impl RingSlice {
    const EMPTY: Self = Self {
        ptr: 0,
        cap: 0,
        start: 0,
        len: 0,
    };
    #[inline]
    pub fn empty() -> Self {
        Self::EMPTY
    }
    #[inline]
    pub fn from(ptr: *const u8, cap: usize, start: usize, end: usize) -> Self {
        assert!(cap == 0 || cap.is_power_of_two(), "not valid cap:{}", cap);
        assert!(end >= start);
        Self {
            ptr: ptr as usize,
            cap,
            start,
            len: end - start,
        }
    }

    #[inline]
    pub fn sub_slice(&self, offset: usize, len: usize) -> RingSlice {
        assert!(offset + len <= self.len());
        Self {
            ptr: self.ptr,
            cap: self.cap,
            start: self.start + offset,
            len,
        }
    }
    // 读取数据. 可能只读取可读数据的一部分。
    #[inline]
    pub fn read(&self, offset: usize) -> &[u8] {
        assert!(offset < self.len());
        let oft = self.mask(self.start + offset);
        let l = (self.cap - oft).min(self.len() - offset);
        unsafe { std::slice::from_raw_parts(self.ptr().offset(oft as isize), l) }
    }
    #[inline]
    pub fn copy_to_vec(&self, v: &mut Vec<u8>) {
        let len = self.len();
        v.reserve(len);
        let mut oft = 0;
        use crate::Buffer;
        while oft < len {
            let data = self.read(oft);
            oft += data.len();
            v.write(data);
        }
    }
    #[inline]
    fn mask(&self, oft: usize) -> usize {
        // 兼容cap是0的场景
        self.cap.wrapping_sub(1) & oft
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }
    #[inline]
    pub fn at(&self, idx: usize) -> u8 {
        assert!(idx < self.len());
        unsafe { *self.oft_ptr(idx) }
    }
    #[inline]
    pub fn update(&mut self, idx: usize, b: u8) {
        assert!(idx < self.len());
        unsafe { *self.oft_ptr(idx) = b }
    }
    #[inline]
    pub fn ptr(&self) -> *mut u8 {
        self.ptr as *mut u8
    }
    #[inline]
    unsafe fn oft_ptr(&self, idx: usize) -> *mut u8 {
        assert!(idx < self.len());
        let oft = self.mask(self.start + idx);
        self.ptr().offset(oft as isize)
    }

    #[inline]
    pub fn find(&self, offset: usize, b: u8) -> Option<usize> {
        for i in offset..self.len() {
            if self.at(i) == b {
                return Some(i);
            }
        }
        None
    }
    // 查找是否存在 '\r\n' ，返回匹配的第一个字节地址
    #[inline]
    pub fn find_lf_cr(&self, offset: usize) -> Option<usize> {
        for i in offset..self.len() - 1 {
            // 先找'\r'
            if self.at(i) == b'\r' {
                // 再验证'\n'
                if self.at(i + 1) == b'\n' {
                    return Some(i);
                }
            }
        }
        None
    }

    // 查找是否以dest字符串作为最前面的字符串
    #[inline]
    pub fn start_with(&self, offset: usize, dest: &[u8]) -> Result<bool> {
        let mut len = dest.len();
        if (self.len() - offset) < dest.len() {
            len = self.len() - offset;
        }

        for i in 0..len {
            if self.at(offset + i) != dest[i] {
                return Ok(false);
            }
        }

        if len == dest.len() {
            return Ok(true);
        }
        Err(Error::new(ErrorKind::Other, "no enough bytes"))
    }

    #[inline]
    fn end(&self) -> usize {
        self.start + self.len
    }

    #[inline]
    pub fn start_with_ignore_case(&self, offset: usize, dest: &[u8]) -> std::io::Result<bool> {
        let mut len = dest.len();
        if self.len() - offset < dest.len() {
            len = self.len() - offset;
        }

        for i in 0..len {
            let c = dest[i] as char;
            // 对于非ascii字母，直接比较，否则忽略大小写比较
            if !c.is_ascii_alphabetic() {
                if self.at(offset + i) != dest[i] {
                    return Ok(false);
                }
            } else {
                let c_lower = c.to_ascii_lowercase() as u8;
                let c_uper = c.to_ascii_uppercase() as u8;
                let src = self.at(offset + i);
                if src != c_lower && src != c_uper {
                    return Ok(false);
                }
            }
        }

        if len == dest.len() {
            return Ok(true);
        }

        Err(Error::new(ErrorKind::Other, "no enough bytes"))
    }
}

unsafe impl Send for RingSlice {}
unsafe impl Sync for RingSlice {}

use std::convert::TryInto;
macro_rules! define_read_number {
    ($fn_name:ident, $type_name:tt) => {
        #[inline]
        pub fn $fn_name(&self, offset: usize) -> $type_name {
            const SIZE: usize = std::mem::size_of::<$type_name>();
            assert!(self.len() >= offset + SIZE);
            unsafe {
                let oft_start = (self.start + offset) & (self.cap - 1);
                let oft_end = self.end() & (self.cap - 1);
                if oft_end > oft_start || self.cap >= oft_start + SIZE {
                    let b = from_raw_parts(self.ptr().offset(oft_start as isize), SIZE);
                    $type_name::from_be_bytes(b[..SIZE].try_into().unwrap())
                } else {
                    // start索引更高
                    // 拐弯了
                    let mut b = [0u8; SIZE];
                    let n = self.cap - oft_start;
                    copy_nonoverlapping(self.ptr().offset(oft_start as isize), b.as_mut_ptr(), n);
                    copy_nonoverlapping(self.ptr(), b.as_mut_ptr().offset(n as isize), SIZE - n);
                    $type_name::from_be_bytes(b)
                }
            }
        }
    };
}

impl RingSlice {
    // big endian
    define_read_number!(read_u8, u8);
    define_read_number!(read_u16, u16);
    define_read_number!(read_u32, u32);
    define_read_number!(read_u64, u64);
}

impl PartialEq<[u8]> for RingSlice {
    #[inline]
    fn eq(&self, other: &[u8]) -> bool {
        if self.len() == other.len() {
            for i in 0..other.len() {
                if self.at(i) != other[i] {
                    return false;
                }
            }
            true
        } else {
            false
        }
    }
}
// 内容相等
impl PartialEq<Self> for RingSlice {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        if self.len() == other.len() {
            for i in 0..other.len() {
                if self.at(i) != other.at(i) {
                    return false;
                }
            }
            true
        } else {
            false
        }
    }
}
impl From<&[u8]> for RingSlice {
    #[inline]
    fn from(s: &[u8]) -> Self {
        // TODO 诸如quite/quit指令的响应无需内容，可能会存在0长度的data，关注是否有副作用 fishermen
        // assert_ne!(s.len(), 0);
        let cap = s.len().next_power_of_two();
        Self::from(s.as_ptr() as *mut u8, cap, 0, s.len())
    }
}

impl Display for RingSlice {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ptr:{} start:{} len:{} cap:{}",
            self.ptr, self.start, self.len, self.cap
        )
    }
}
impl Debug for RingSlice {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use crate::Utf8;
        let slice = self.sub_slice(0, 512.min(self.len()));
        let data = Vec::with_capacity(slice.len());
        write!(
            f,
            "ptr:{} start:{} len:{} cap:{} => {:?}",
            self.ptr,
            self.start,
            self.len,
            self.cap,
            data.utf8()
        )
    }
}
