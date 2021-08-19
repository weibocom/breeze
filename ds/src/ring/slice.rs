use std::ptr::copy_nonoverlapping;
use std::slice::from_raw_parts;

use crate::Slice;

pub struct RingSlice {
    // ptr: *const u8,
    // 在trim时，需要进行进行修改
    ptr: *mut u8,
    cap: usize,
    start: usize,
    offset: usize,
    end: usize,
}

impl Default for RingSlice {
    fn default() -> Self {
        RingSlice {
            ptr: 0 as *mut u8,
            start: 0,
            offset: 0,
            end: 0,
            cap: 0,
        }
    }
}

impl RingSlice {
    #[inline(always)]
    //pub fn from(ptr: *const u8, cap: usize, start: usize, end: usize) -> Self {
    // ptr参数需要是mut
    pub fn from(ptr: *mut u8, cap: usize, start: usize, end: usize) -> Self {
        debug_assert!(cap > 0);
        debug_assert_eq!(cap, cap.next_power_of_two());
        Self {
            ptr: ptr,
            cap: cap,
            start: start,
            offset: start,
            end: end,
        }
    }
    #[inline(always)]
    pub fn sub_slice(&self, offset: usize, len: usize) -> RingSlice {
        debug_assert!(offset < self.len());
        debug_assert!(offset + len < self.len());
        Self::from(
            self.ptr,
            self.cap,
            self.offset + offset,
            self.offset + offset + len,
        )
    }
    #[inline(always)]
    pub fn resize(&mut self, num: usize) {
        debug_assert!(self.len() >= num);
        self.end = self.start + num;
    }

    #[inline(always)]
    pub fn take_slice(&mut self) -> Slice {
        debug_assert!(self.cap > 0);
        let s = self.next_slice();
        self.advance(s.len());
        s
    }

    #[inline(always)]
    fn next_slice(&self) -> Slice {
        debug_assert!(self.cap > 0);
        let oft = self.offset & (self.cap - 1);
        let l = (self.cap - oft).min(self.available());
        unsafe { Slice::new(self.ptr.offset(oft as isize) as usize, l) }
    }
    #[inline(always)]
    fn advance(&mut self, n: usize) {
        debug_assert!(self.offset + n <= self.end);
        self.offset += n;
    }

    // 从offset读取len个字节
    pub fn read_bytes(&self, offset: usize, len: usize) -> String {
        debug_assert!(self.available() >= offset + len);
        let oft_start = (self.offset + offset) & (self.cap - 1);
        let oft_end = self.end & (self.cap - 1);

        if oft_end > oft_start || self.cap >= oft_start + len {
            unsafe {
                let b = from_raw_parts(self.ptr.offset(oft_start as isize), len);
                String::from_utf8_lossy(b).to_string()
            }
        } else {
            // start 索引更高，字节转弯了
            // let mut result = String::with_capacity(len);
            let mut bytes: Vec<u8> = Vec::with_capacity(len);
            let n = self.cap - oft_start;
            unsafe {
                copy_nonoverlapping(self.ptr.offset(oft_start as isize), bytes.as_mut_ptr(), n);
                copy_nonoverlapping(self.ptr, bytes.as_mut_ptr().offset(n as isize), len - n);

                bytes.set_len(len);
                let result = String::from_utf8_lossy(&bytes).to_string();
                result
            }
        }
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        unsafe { from_raw_parts(self.ptr, self.cap) }
    }

    #[inline(always)]
    pub fn available(&self) -> usize {
        self.end - self.offset
    }
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.end - self.start
    }
    #[inline(always)]
    pub fn at(&self, idx: usize) -> u8 {
        debug_assert!(idx < self.len());
        unsafe {
            *self
                .ptr
                .offset(((self.offset + idx) & (self.cap - 1)) as isize)
        }
    }

    // TODO 这里self不设为mut，否则会造成一系列震荡，但一定要特别注意unsafe里面的逻辑  fishermen
    pub fn update_byte(&self, idx: usize, new_value: u8) {
        debug_assert!(idx < self.len());
        let pos = (self.offset + idx) & (self.cap - 1);
        unsafe {
            *self.ptr.offset(pos as isize) = new_value;
        }
    }

    #[inline(always)]
    pub fn location(&self) -> (usize, usize) {
        (self.start, self.end)
    }
    // 从offset开始，查找s是否存在
    // 最坏时间复杂度 O(self.len() * s.len())
    // 但通常在协议处理过程中，被查的s都是特殊字符，而且s的长度通常比较小，因为时间复杂度会接近于O(self.len())
    pub fn index_of(&self, offset: usize, s: &[u8]) -> Option<usize> {
        let mut i = offset;
        while i + s.len() <= self.len() {
            for j in 0..s.len() {
                if self.at(i + j) != s[j] {
                    i += 1;
                    continue;
                }
            }
            return Some(i);
        }
        None
    }
    // 查找是否存在 '\r\n' ，返回匹配的第一个字节地址
    pub fn index_lf_cr(&self, offset: usize) -> Option<usize> {
        self.index_of(offset, &[b'\r', b'\n'])
    }
}

unsafe impl Send for RingSlice {}
unsafe impl Sync for RingSlice {}

use std::convert::TryInto;
macro_rules! define_read_number {
    ($fn_name:ident, $type_name:tt) => {
        pub fn $fn_name(&self, offset: usize) -> $type_name {
            const SIZE: usize = std::mem::size_of::<$type_name>();
            debug_assert!(self.available() >= offset + SIZE);
            unsafe {
                let oft_start = (self.offset + offset) & (self.cap - 1);
                let oft_end = self.end & (self.cap - 1);
                if oft_end > oft_start || self.cap >= oft_start + SIZE {
                    let b = from_raw_parts(self.ptr.offset(oft_start as isize), SIZE);
                    $type_name::from_be_bytes(b[..SIZE].try_into().unwrap())
                } else {
                    // start索引更高
                    // 拐弯了
                    let mut b = [0u8; SIZE];
                    let n = self.cap - oft_start;
                    copy_nonoverlapping(self.ptr.offset(oft_start as isize), b.as_mut_ptr(), n);
                    copy_nonoverlapping(self.ptr, b.as_mut_ptr().offset(n as isize), SIZE - n);
                    $type_name::from_be_bytes(b)
                }
            }
        }
    };
}

impl RingSlice {
    // big endian
    define_read_number!(read_u16, u16);
    define_read_number!(read_u32, u32);
    define_read_number!(read_u64, u64);
}

impl PartialEq<[u8]> for RingSlice {
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
