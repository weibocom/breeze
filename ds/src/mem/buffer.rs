use super::RingSlice;

use std::mem::ManuallyDrop;
use std::ptr::NonNull;
use std::slice::from_raw_parts_mut;

// <= read的字节是已经全部读取完的
// [read, write]是已经写入，但未全部收到读取完成通知的
// write是当前写入的地址
// write..size是可写区域
pub struct RingBuffer {
    data: NonNull<u8>,
    size: usize,
    read: usize,
    write: usize,
}

impl RingBuffer {
    // 如果size是0，则调用者需要确保数据先写入，再读取。
    // 否则可能导致读取时，size - 1越界
    pub fn with_capacity(size: usize) -> Self {
        assert!(size == 0 || size.is_power_of_two(), "{} not valid", size);
        let mut data = ManuallyDrop::new(Vec::with_capacity(size));
        assert_eq!(size, data.capacity());
        let ptr = unsafe { NonNull::new_unchecked(data.as_mut_ptr()) };
        super::BUF_RX.incr_by(size);
        Self {
            size,
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
    pub(super) fn advance_read(&mut self, n: usize) {
        debug_assert!(n <= self.len());
        self.read += n;
    }
    #[inline(always)]
    pub(super) fn writtened(&self) -> usize {
        self.write
    }
    #[inline(always)]
    fn advance_write(&mut self, n: usize) {
        self.write += n;
    }
    #[inline(always)]
    fn mask(&self, offset: usize) -> usize {
        // 兼容size为0的场景
        offset & self.size.wrapping_sub(1)
    }
    #[inline]
    pub fn copy_from<O, R: crate::BuffRead<Out = O>>(&mut self, src: &mut R) -> O {
        let oft = self.mask(self.write);
        let avail = self.available();
        let n = (self.size - oft).min(avail);
        let b = unsafe { from_raw_parts_mut(self.data.as_ptr().add(oft), n) };
        let (read, out) = src.read(b);
        self.advance_write(read);
        // read < n：buffer未满，则认定为已读取完毕
        // avail == read:  当前buffer已满.
        if read < n || avail == read {
            return out;
        }
        // 运行到这说明：buf分段，且未满
        // 从0开始读取
        let b = unsafe { from_raw_parts_mut(self.data.as_ptr(), avail - read) };
        let (read, out) = src.read(b);
        self.advance_write(read);
        out
    }
    //// 返回可写入的buffer。如果无法写入，则返回一个长度为0的slice
    //#[inline]
    //fn as_mut_bytes(&mut self) -> &mut [u8] {
    //    if self.read + self.size == self.write {
    //        // 已满
    //        unsafe { from_raw_parts_mut(self.data.as_ptr(), 0) }
    //    } else {
    //        let offset = self.mask(self.write);
    //        let read = self.mask(self.read);
    //        let n = if offset < read {
    //            read - offset
    //        } else {
    //            self.size - offset
    //        };
    //        unsafe { from_raw_parts_mut(self.data.as_ptr().offset(offset as isize), n) }
    //    }
    //}
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
    // 可以读写的数据长度
    #[inline]
    pub fn len(&self) -> usize {
        debug_assert!(self.write >= self.read);
        self.write - self.read
    }
    #[inline]
    pub fn available(&self) -> usize {
        self.size - self.len()
    }
    // 调用方确保buf.available() >= rs.len()，否则UB
    #[inline]
    unsafe fn write_all(&mut self, rs: &RingSlice) {
        use std::ptr::copy_nonoverlapping as copy;
        debug_assert!(rs.len() <= self.available());
        // 写入的位置
        rs.visit_seg(0, |p, l| {
            let offset = self.mask(self.write);
            let n = l.min(self.size - offset);
            copy(p, self.data.as_ptr().add(offset), n);
            if n < l {
                copy(p.add(n), self.data.as_ptr(), l - n);
            }
            self.advance_write(l);
        });
    }

    // cap > self.len()
    #[inline]
    pub(crate) fn resize(&self, cap: usize) -> Self {
        assert!(cap >= self.write - self.read);
        assert!(cap.is_power_of_two());
        let mut new = Self::with_capacity(cap);
        new.read = self.read;
        new.write = self.read;
        if self.len() > 0 {
            assert!(new.available() >= self.len());
            unsafe { new.write_all(&self.data()) };
        }
        assert_eq!(self.write, new.write);
        assert_eq!(self.read, new.read);
        new
    }
}

impl Drop for RingBuffer {
    fn drop(&mut self) {
        super::BUF_RX.decr_by(self.size);
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

mod tests {
    impl super::RingBuffer {
        pub fn consume(&mut self, n: usize) {
            assert!(self.len() >= n);
            self.advance_read(n);
        }
        #[inline]
        pub fn write(&mut self, data: &crate::RingSlice) -> usize {
            let n = data.len().min(self.available());
            unsafe { self.write_all(&data.slice(0, n)) };
            n
        }
    }
}

unsafe impl Send for RingBuffer {}
unsafe impl Sync for RingBuffer {}
