use std::ptr::copy_nonoverlapping;
use std::slice::from_raw_parts;

use super::Slice;

use byteorder::{BigEndian, ByteOrder};

pub struct RingSlice {
    ptr: *const u8,
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
    pub fn from(ptr: *const u8, cap: usize, start: usize, end: usize) -> Self {
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
    pub fn next_slice(&self) -> Slice {
        debug_assert!(self.cap > 0);
        let oft = self.offset & (self.cap - 1);
        let l = (self.cap - oft).min(self.available());
        unsafe { Slice::new(self.ptr.offset(oft as isize) as usize, l) }
    }
    #[inline(always)]
    pub fn advance(&mut self, n: usize) {
        debug_assert!(self.offset + n <= self.end);
        self.offset += n;
    }

    // 调用方确保len >= offset + 4
    pub fn read_u32(&self, offset: usize) -> u32 {
        debug_assert!(self.available() >= offset + 4);
        unsafe {
            let oft_start = (self.offset + offset) & (self.cap - 1);
            let oft_end = self.end & (self.cap - 1);
            if oft_end > oft_start || self.cap >= oft_start + 4 {
                let b = from_raw_parts(self.ptr.offset(oft_start as isize), 4);
                BigEndian::read_u32(b)
            } else {
                // start索引更高
                // 4个字节拐弯了
                let mut b = [0u8; 4];
                let n = self.cap - oft_start;
                copy_nonoverlapping(self.ptr.offset(oft_start as isize), b.as_mut_ptr(), n);
                copy_nonoverlapping(self.ptr, b.as_mut_ptr().offset(n as isize), 4 - n);
                BigEndian::read_u32(&b)
            }
        }
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
    #[inline(always)]
    pub fn location(&self) -> (usize, usize) {
        (self.start, self.end)
    }
    // 从offset开始，查找s是否存在
    // 最坏时间复杂度 O(self.len() * s.len())
    // 但通常在协议处理过程中，被查的s都是特殊字符，而且s的长度通常比较小，因为时间复杂度会接近于O(self.len())
    pub fn index(&self, offset: usize, s: &[u8]) -> Option<usize> {
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
        self.index(offset, &[b'\r', b'\n'])
    }
}

unsafe impl Send for RingSlice {}
unsafe impl Sync for RingSlice {}
