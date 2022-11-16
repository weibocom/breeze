use std::fmt::{Debug, Display, Formatter};
use std::io::{Error, ErrorKind, Result};
use std::ptr::copy_nonoverlapping;
use std::slice::from_raw_parts;

#[derive(Default)]
pub struct RingSlice {
    ptr: usize,
    cap: usize,
    start: usize,
    end: usize,
}

impl RingSlice {
    #[inline]
    pub fn from(ptr: *const u8, cap: usize, start: usize, end: usize) -> Self {
        assert!(cap > 0);
        assert_eq!(cap & cap - 1, 0);
        assert!(end >= start);
        Self {
            ptr: ptr as usize,
            cap,
            start,
            end,
        }
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.cap
    }

    #[inline]
    pub fn sub_slice(&self, offset: usize, len: usize) -> RingSlice {
        assert!(offset + len <= self.len());
        Self::from(
            self.ptr(),
            self.cap,
            self.start + offset,
            self.start + offset + len,
        )
    }
    // 读取数据. 可能只读取可读数据的一部分。
    #[inline]
    pub fn read(&self, offset: usize) -> &[u8] {
        assert!(offset < self.len());
        let oft = self.mask(self.start + offset);
        let l = (self.cap - oft).min(self.end - self.start - offset);
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
        (self.cap - 1) & oft
    }

    #[inline]
    pub fn len(&self) -> usize {
        assert!(self.end >= self.start);
        self.end - self.start
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

    //pub fn split(&self, splitter: &[u8]) -> Vec<Self> {
    //    let mut pos = 0 as usize;
    //    let mut result: Vec<RingSlice> = vec![];
    //    loop {
    //        let new_pos = self.find_sub(pos, splitter);
    //        if new_pos.is_none() {
    //            if pos < self.len() {
    //                result.push(self.sub_slice(pos, self.len() - pos));
    //            }
    //            return result;
    //        } else {
    //            let new_pos = new_pos.unwrap();
    //            result.push(self.sub_slice(pos, new_pos));
    //            if new_pos + splitter.len() == self.end - self.start {
    //                return result;
    //            }
    //            pos = pos + new_pos + splitter.len();
    //        }
    //    }
    //}
    #[inline]
    pub fn find(&self, offset: usize, b: u8) -> Option<usize> {
        for i in offset..self.len() {
            if self.at(i) == b {
                return Some(i);
            }
        }
        None
    }
    //// 从offset开始，查找s是否存在
    //// 最坏时间复杂度 O(self.len() * s.len())
    //// 但通常在协议处理过程中，被查的s都是特殊字符，而且s的长度通常比较小，因为时间复杂度会接近于O(self.len())
    //pub fn find_sub(&self, offset: usize, s: &[u8]) -> Option<usize> {
    //    if self.start + offset + s.len() > self.end {
    //        return None;
    //    }
    //    let mut i = 0 as usize;
    //    let i_cap = self.len() - s.len() - offset;
    //    while i <= i_cap {
    //        let mut found_len = 0 as usize;
    //        for j in 0..s.len() {
    //            if self.read_u8(offset + i + j) != s[j] {
    //                i += 1;
    //                break;
    //            } else {
    //                found_len = found_len + 1;
    //            }
    //        }
    //        if found_len == s.len() {
    //            return Some(i);
    //        }
    //    }
    //    None
    //}
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

    // 只用来debug
    #[inline]
    fn to_vec(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(self.len());
        self.copy_to_vec(&mut v);
        v
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
                let oft_end = self.end & (self.cap - 1);
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
        let len = s.len();
        let cap = len.next_power_of_two();
        Self::from(s.as_ptr() as *mut u8, cap, 0, s.len())
    }
}

impl Display for RingSlice {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ptr:{} start:{} end:{} cap:{}",
            self.ptr, self.start, self.end, self.cap
        )
    }
}
impl Debug for RingSlice {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use crate::Utf8;
        let data = if self.len() > 1024 {
            format!("[hidden for too long len:{}]", self.len())
        } else {
            self.to_vec().utf8()
        };

        write!(
            f,
            "ptr:{} start:{} end:{} cap:{} => {:?}",
            self.ptr, self.start, self.end, self.cap, data
        )
    }
}
