use std::mem::{transmute, MaybeUninit};
use std::ptr::copy_nonoverlapping;
use std::slice::from_raw_parts;

use byteorder::{BigEndian, ByteOrder};

use tokio::io::ReadBuf;

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
        debug_assert_eq!(cap, cap.next_power_of_two());
        Self {
            ptr: ptr,
            cap: cap,
            start: start,
            offset: start,
            end: end,
        }
    }
    #[inline]
    fn mask(&self, offset: usize) -> usize {
        self.offset & (self.cap - 1)
    }
    // 返回true，说明数据已经读完了
    pub fn read(&mut self, buff: &mut ReadBuf) -> bool {
        unsafe {
            if self.end > self.offset {
                let oft_start = self.offset & (self.cap - 1);
                let oft_end = self.end & (self.cap - 1);

                let n = buff
                    .remaining()
                    .min(self.cap - oft_start)
                    .min(self.available());
                let bytes = transmute::<&mut [MaybeUninit<u8>], &mut [u8]>(buff.unfilled_mut());
                copy_nonoverlapping(self.ptr.offset(oft_start as isize), bytes.as_mut_ptr(), n);
                buff.advance(n);
                self.offset += n;

                // 说明可写入的信息写到到数据末尾，需要再次写入
                if buff.remaining() > 0 && self.end > self.offset {
                    let n2 = buff.remaining().min(oft_end);
                    copy_nonoverlapping(self.ptr, bytes.as_mut_ptr().offset(n as isize), n2);
                    buff.advance(n2);
                    self.offset += n2;
                }
            }

            self.offset == self.end
        }
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
    pub fn available(&self) -> usize {
        self.end - self.offset
    }
    pub fn len(&self) -> usize {
        self.end - self.start
    }
    pub fn at(&self, idx: usize) -> u8 {
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
    pub fn index(&self, _offset: usize, _s: &[u8]) -> Option<usize> {
        todo!();
    }
    // 查找是否存在 '\r\n' ，返回匹配的第一个字节地址
    pub fn index_lf_cr(&self, offset: usize) -> Option<usize> {
        todo!();
    }
}

#[cfg(test)]
mod tests {
    use super::RingSlice;
    use tokio::io::ReadBuf;
    #[test]
    fn test_ring_slice() {
        let cap = 1024;
        let mut data: Vec<u8> = (0..cap).map(|_| rand::random::<u8>()).collect();
        let dc = data.clone();
        let ptr = data.as_mut_ptr();
        std::mem::forget(data);
        let mut in_range = RingSlice::from(ptr, cap, 0, 32);
        let mut buf = vec![0u8; cap];
        let mut read_buf = ReadBuf::new(&mut buf);
        in_range.read(&mut read_buf);
        assert_eq!(in_range.available(), 0);
        assert_eq!(read_buf.filled(), &dc[in_range.start..in_range.end]);

        // 截止到末尾的
        let mut end_range = RingSlice::from(ptr, cap, cap - 32, cap);
        read_buf.clear();
        assert!(end_range.read(&mut read_buf));
        assert_eq!(end_range.available(), 0);
        assert_eq!(read_buf.filled(), &dc[end_range.start..end_range.end]);

        let mut over_range = RingSlice::from(ptr, cap, cap - 32, cap + 32);
        read_buf.clear();
        assert!(over_range.read(&mut read_buf));
        assert_eq!(over_range.available(), 0);
        let mut merged = (&dc[cap - 32..]).clone().to_vec();
        merged.extend(&dc[0..32]);
        assert_eq!(read_buf.filled(), &merged);

        let u32_num = 111234567u32;
        let bytes = u32_num.to_be_bytes();
        unsafe {
            println!("bytes:{:?}", bytes);
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
