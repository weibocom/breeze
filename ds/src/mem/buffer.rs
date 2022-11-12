use super::RingSlice;

use std::ptr::NonNull;
use std::slice::from_raw_parts_mut;

// <= read的字节是已经全部读取完的
// [read, processed]是已经写入，但未全部收到读取完成通知的
// write是当前写入的地址
pub struct RingBuffer {
    data: NonNull<u8>,
    size: usize,
    read: usize,
    write: usize,
}

impl RingBuffer {
    pub fn with_capacity(size: usize) -> Self {
        let buff_size = size.next_power_of_two();
        let mut data = Vec::with_capacity(buff_size);
        let ptr = unsafe { NonNull::new_unchecked(data.as_mut_ptr()) };
        std::mem::forget(data);
        Self {
            size: buff_size,
            data: ptr,
            read: 0,
            write: 0,
        }
    }
    #[inline]
    pub fn read(&self) -> usize {
        self.read
    }
    #[inline]
    pub fn advance_read(&mut self, n: usize) {
        assert!(n <= self.len());
        self.read += n;
    }
    #[inline]
    pub fn writtened(&self) -> usize {
        self.write
    }
    #[inline]
    pub fn advance_write(&mut self, n: usize) {
        self.write += n;
    }
    #[inline]
    fn mask(&self, offset: usize) -> usize {
        offset & (self.size - 1)
    }
    // 返回可写入的buffer。如果无法写入，则返回一个长度为0的slice
    #[inline]
    pub fn as_mut_bytes(&mut self) -> &mut [u8] {
        let offset = self.mask(self.write);
        let n = if self.read + self.size == self.write {
            // 已满
            0
        } else {
            let read = self.mask(self.read);
            if offset < read {
                read - offset
            } else {
                self.size - offset
            }
        };
        unsafe { from_raw_parts_mut(self.data.as_ptr().offset(offset as isize), n) }
    }
    #[inline]
    pub fn data(&self) -> RingSlice {
        RingSlice::from(self.data.as_ptr(), self.size, self.read, self.write)
    }
    // 从指定位置开始的数据
    #[inline]
    pub fn slice(&self, read: usize, len: usize) -> RingSlice {
        assert!(read >= self.read);
        assert!(read + len <= self.write);
        RingSlice::from(self.data.as_ptr(), self.size, read, read + len)
    }
    #[inline]
    pub fn cap(&self) -> usize {
        self.size
    }
    #[inline]
    pub fn len(&self) -> usize {
        assert!(self.write >= self.read);
        self.write - self.read
    }
    #[inline]
    pub fn write(&mut self, data: &RingSlice) -> usize {
        let mut w = 0;
        while w < data.len() {
            let src = data.read(w);
            assert!(src.len() > 0);
            let dst = self.as_mut_bytes();
            if dst.len() == 0 {
                break;
            }
            let l = src.len().min(dst.len());
            use std::ptr::copy_nonoverlapping as copy;
            unsafe { copy(src.as_ptr(), dst.as_mut_ptr(), l) };
            self.advance_write(l);
            w += l;
        }
        w
    }

    // cap > self.len()
    #[inline]
    pub(crate) fn resize(&self, cap: usize) -> Self {
        assert!(cap >= self.write - self.read);
        let mut new = Self::with_capacity(cap);
        new.read = self.read;
        new.write = self.read;
        new.write(&self.data());
        assert_eq!(self.write, new.write);
        assert_eq!(self.read, new.read);
        new
    }

    #[inline]
    pub fn update(&mut self, idx: usize, val: u8) {
        assert!(idx < self.len());
        unsafe {
            *self
                .data
                .as_ptr()
                .offset(self.mask(self.read + idx) as isize) = val
        }
    }
    #[inline]
    pub fn at(&self, idx: usize) -> u8 {
        assert!(idx < self.len());
        unsafe {
            *self
                .data
                .as_ptr()
                .offset(self.mask(self.read + idx) as isize)
        }
    }
}

impl Drop for RingBuffer {
    fn drop(&mut self) {
        unsafe {
            let _ = Vec::from_raw_parts(self.data.as_ptr(), 0, self.size);
        }
    }
}

use std::fmt::{self, Debug, Display, Formatter};
impl Display for RingBuffer {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "rb:(cap:{} read:{} write:{})",
            self.size, self.read, self.write
        )
    }
}
impl Debug for RingBuffer {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "rb:(ptr:{:#x} cap:{} read:{} write:{})",
            self.data.as_ptr() as usize,
            self.size,
            self.read,
            self.write
        )
    }
}

unsafe impl Send for RingBuffer {}
unsafe impl Sync for RingBuffer {}
