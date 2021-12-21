use std::ptr::copy_nonoverlapping;
use std::slice::from_raw_parts;

#[derive(Debug, Default)]
pub struct RingSlice {
    ptr: usize,
    cap: usize,
    start: usize,
    end: usize,
}

impl RingSlice {
    #[inline(always)]
    pub fn from(ptr: *const u8, cap: usize, start: usize, end: usize) -> Self {
        debug_assert!(cap > 0);
        debug_assert_eq!(cap, cap.next_power_of_two());
        debug_assert!(end >= start);
        let me = Self {
            ptr: ptr as usize,
            cap: cap,
            start: start,
            end: end,
        };
        me
    }
    #[inline(always)]
    pub fn sub_slice(&self, offset: usize, len: usize) -> RingSlice {
        debug_assert!(offset + len <= self.len());
        Self::from(
            self.ptr(),
            self.cap,
            self.start + offset,
            self.start + offset + len,
        )
    }
    // 从start开始的所有数据。start不是offset，是绝对值。
    #[inline(always)]
    pub fn take(&self, start: usize) -> RingSlice {
        debug_assert!(start >= self.start);
        debug_assert!(start <= self.end);
        Self::from(self.ptr(), self.cap, start, self.end)
    }
    // 读取数据. 可能只读取可读数据的一部分。
    #[inline(always)]
    pub fn read(&self, offset: usize) -> &[u8] {
        debug_assert!(offset < self.len());
        let oft = self.mask(self.start + offset);
        let l = (self.cap - oft).min(self.end - self.start - offset);
        //println!("read data offset:{} start offset:{} l:{}", offset, oft, l);
        unsafe { std::slice::from_raw_parts(self.ptr().offset(oft as isize), l) }
    }
    #[inline(always)]
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

    #[inline(always)]
    fn mask(&self, oft: usize) -> usize {
        (self.cap - 1) & oft
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        debug_assert!(self.end >= self.start);
        self.end - self.start
    }
    #[inline(always)]
    pub fn at(&self, idx: usize) -> u8 {
        debug_assert!(idx < self.len());
        unsafe { *self.oft_ptr(idx) }
    }
    #[inline(always)]
    pub fn update(&mut self, idx: usize, b: u8) {
        debug_assert!(idx < self.len());
        unsafe { *self.oft_ptr(idx) = b }
    }
    #[inline(always)]
    pub fn ptr(&self) -> *mut u8 {
        self.ptr as *mut u8
    }
    #[inline(always)]
    unsafe fn oft_ptr(&self, idx: usize) -> *mut u8 {
        debug_assert!(idx < self.len());
        let oft = self.mask(self.start + idx);
        self.ptr().offset(oft as isize)
    }

    pub fn split(&self, splitter: &[u8]) -> Vec<Self> {
        let mut pos = 0 as usize;
        let mut result: Vec<RingSlice> = vec![];
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
                if new_pos + splitter.len() == self.end - self.start {
                    return result;
                }
                pos = pos + new_pos + splitter.len();
            }
        }
    }
    // 从offset开始，查找s是否存在
    // 最坏时间复杂度 O(self.len() * s.len())
    // 但通常在协议处理过程中，被查的s都是特殊字符，而且s的长度通常比较小，因为时间复杂度会接近于O(self.len())
    pub fn find_sub(&self, offset: usize, s: &[u8]) -> Option<usize> {
        if self.start + offset + s.len() > self.end {
            return None;
        }
        let mut i = 0 as usize;
        let i_cap = self.len() - s.len() - offset;
        while i <= i_cap {
            let mut found_len = 0 as usize;
            for j in 0..s.len() {
                if self.read_u8(offset + i + j) != s[j] {
                    i += 1;
                    break;
                } else {
                    found_len = found_len + 1;
                }
            }
            if found_len == s.len() {
                return Some(i);
            }
        }
        None
    }
    // 查找是否存在 '\r\n' ，返回匹配的第一个字节地址
    pub fn index_lf_cr(&self, offset: usize) -> Option<usize> {
        self.find_sub(offset, &[b'\r', b'\n'])
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
            debug_assert!(self.len() >= offset + SIZE);
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
impl From<&[u8]> for RingSlice {
    #[inline]
    fn from(s: &[u8]) -> Self {
        debug_assert_ne!(s.len(), 0);
        let len = s.len();
        let cap = len.next_power_of_two();
        Self::from(s.as_ptr() as *mut u8, cap, 0, s.len())
    }
}
use std::fmt;
use std::fmt::{Display, Formatter};
impl Display for RingSlice {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ptr:{} start:{} end:{} cap:{}",
            self.ptr, self.start, self.end, self.cap
        )
    }
}
