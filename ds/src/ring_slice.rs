use std::iter::FromIterator;
use std::ptr::copy_nonoverlapping;
use std::slice::from_raw_parts;
use std::str::FromStr;

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
    pub fn resize(&mut self, num: usize) {
        debug_assert!(self.len() >= num);
        self.end = self.start + num;
    }

    pub fn next_slice(&self) -> Slice {
        debug_assert!(self.cap > 0);
        let oft = self.offset & (self.cap - 1);
        let l = (self.cap - oft).min(self.available());
        unsafe { Slice::new(self.ptr.offset(oft as isize) as usize, l) }
    }
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
    pub fn read_u16(&self, offset: usize) -> u16 {
        debug_assert!(self.available() >= offset + 2);
        let oft_start = (self.offset + offset) & (self.cap - 1);
        let oft_end = self.end & (self.cap - 1);
        if oft_end > oft_start || self.cap >= oft_start + 2 {
            unsafe {
                let b = from_raw_parts(self.ptr.offset(oft_start as isize), 2);
                BigEndian::read_u16(b)
            }
        } else {
            // start 索引更高，2个字节转弯了
            let mut b = [0u8, 2];
            let n = self.cap - oft_start;
            unsafe {
                copy_nonoverlapping(self.ptr.offset(oft_start as isize), b.as_mut_ptr(), n);
                copy_nonoverlapping(self.ptr, b.as_mut_ptr().offset(n as isize), 2 - n);
                BigEndian::read_u16(&b)
            }
        }
    }
    // 从offset读取len个字节
    pub fn read_bytes(&self, offset: usize, len: usize) -> String {
        debug_assert!(self.available() >= offset + len);
        let oft_start = (self.offset + offset) & (self.cap - 1);
        let oft_end = self.end & (self.cap - 1);
        let result = String::new();
        if oft_end > oft_start || self.cap >= oft_start + len {
            unsafe {
                let b = from_raw_parts(self.ptr.offset(oft_start as isize), len);
                String::from_utf8_lossy(b).to_string()
            }
        } else {
            // start 索引更高，2个字节转弯了
            let mut result = String::with_capacity(len);
            let n = self.cap - oft_start;
            unsafe {
                copy_nonoverlapping(self.ptr.offset(oft_start as isize), result.as_mut_ptr(), n);
                copy_nonoverlapping(self.ptr, result.as_mut_ptr().offset(n as isize), len - n);
                result
            }
        }
    }
    pub fn available(&self) -> usize {
        self.end - self.offset
    }
    pub fn len(&self) -> usize {
        self.end - self.start
    }
    pub fn at(&self, idx: usize) -> u8 {
        debug_assert!(idx < self.len());
        unsafe {
            *self
                .ptr
                .offset(((self.offset + idx) & (self.cap - 1)) as isize)
        }
    }
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

#[cfg(test)]
mod tests_ds {
    use super::RingSlice;
    #[test]
    fn test_ring_slice() {
        let cap = 1024;
        let mut data: Vec<u8> = (0..cap).map(|_| rand::random::<u8>()).collect();
        let dc = data.clone();
        let ptr = data.as_mut_ptr();
        std::mem::forget(data);
        let mut in_range = RingSlice::from(ptr, cap, 0, 32);
        let mut buf = vec![0u8; cap];
        let n = in_range.read(&mut buf);
        assert_eq!(in_range.available(), 0);
        assert_eq!(&buf[..n], &dc[in_range.start..in_range.end]);

        // 截止到末尾的
        let mut end_range = RingSlice::from(ptr, cap, cap - 32, cap);
        let n = end_range.read(&mut buf);
        assert_eq!(end_range.available(), 0);
        assert_eq!(&buf[0..n], &dc[end_range.start..end_range.end]);

        let mut over_range = RingSlice::from(ptr, cap, cap - 32, cap + 32);
        let n = over_range.read(&mut buf);
        assert_eq!(over_range.available(), 0);
        let mut merged = (&dc[cap - 32..]).clone().to_vec();
        merged.extend(&dc[0..32]);
        assert_eq!(&buf[0..n], &merged);

        let u32_num = 111234567u32;
        let bytes = u32_num.to_be_bytes();
        unsafe {
            log::debug!("bytes:{:?}", bytes);
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr.offset(8), 4);
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr.offset(1023), 1);
            std::ptr::copy_nonoverlapping(bytes.as_ptr().offset(1), ptr, 3);
        }
        let num_range = RingSlice::from(ptr, cap, 1000, 1064);
        assert_eq!(u32_num, num_range.read_u32(32));

        assert_eq!(u32_num, num_range.read_u32(23));

        let _ = unsafe { Vec::from_raw_parts(ptr, 0, cap) };
    }
}
