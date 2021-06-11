use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

pub trait OffsetSequence {
    fn next(&self, offset: usize) -> usize;
}

// 提供单调访问的buffer。访问buffer的offset是单调增加的
// 支持多写一读。
pub struct MonoRingBuffer {
    // 这个偏移量是全局递增的偏移量。不是data内部的偏移量
    r_offset: AtomicUsize,
    // 这个偏移量是全局递增的偏移量。不是data内部的偏移量
    w_offset: AtomicUsize,
    // 上一次取数据后，因为data取到末尾，没取完，所以缓存末尾的位置
    fetch_offset: AtomicU32,
    data: NonNull<u8>,
    size: usize,
}

unsafe impl Send for MonoRingBuffer {}
unsafe impl Sync for MonoRingBuffer {}

impl Drop for MonoRingBuffer {
    fn drop(&mut self) {
        unsafe {
            let _ = Vec::from_raw_parts(self.data.as_ptr(), 0, self.size);
        }
    }
}

impl MonoRingBuffer {
    pub fn with_capacity(cap: usize) -> Self {
        let cap = cap.next_power_of_two();
        let mut data = Vec::with_capacity(cap);
        let ptr = unsafe { NonNull::new_unchecked(data.as_mut_ptr()) };
        std::mem::forget(data);
        Self {
            size: cap,
            data: ptr,
            r_offset: AtomicUsize::new(0),
            w_offset: AtomicUsize::new(0),
            fetch_offset: AtomicU32::new(0),
        }
    }
    #[inline]
    pub fn reserve(&self, len: usize) -> usize {
        self.w_offset.fetch_add(len, Ordering::AcqRel)
    }
    #[inline]
    pub fn write(&self, offset: usize, buf: &[u8]) -> bool {
        debug_assert!(buf.len() < self.size);
        let read = self.r_offset.load(Ordering::Acquire);
        if read + self.size < offset + buf.len() {
            return false;
        }
        use std::ptr::copy_nonoverlapping as copy;
        let offset = self.convert_offset(offset);
        unsafe {
            if self.size - offset >= buf.len() {
                copy(
                    buf.as_ptr(),
                    self.data.as_ptr().offset(offset as isize),
                    buf.len(),
                );
            } else {
                // 要分两次写入
                // 第一次从offset写入 self.size - offset个
                // 第二次从0开始写入剩下的
                let first = self.size - offset;
                copy(
                    buf.as_ptr(),
                    self.data.as_ptr().offset(offset as isize),
                    first,
                );
                copy(
                    buf.as_ptr().offset(first as isize),
                    self.data.as_ptr(),
                    buf.len() - first,
                );
            }
        }
        true
    }
    // 返回从read开始到write的连续数据。
    // 因为数据写入是先reserve再write，所以可能会有空洞。因此需要额外的判断。
    pub fn fetch<S>(&self, seq: &S) -> Option<&[u8]>
    where
        S: OffsetSequence,
    {
        unsafe {
            use std::slice::from_raw_parts;
            let last = self.fetch_offset.load(Ordering::Acquire);
            if last > 0 {
                self.fetch_offset.store(0, Ordering::Release);
                return Some(from_raw_parts(self.data.as_ptr(), last as usize));
            }
            let current = self.r_offset.load(Ordering::Acquire);
            let last_read = current;
            let current = seq.next(current);
            if current == last_read {
                return None;
            }
            let start = self.convert_offset(last_read);
            let end = self.convert_offset(current);
            if end > start {
                Some(from_raw_parts(
                    self.data.as_ptr().offset(start as isize),
                    end - start,
                ))
            } else {
                // 先从read_offset到结尾
                // 再从0到end的，下一次请求再获取
                self.fetch_offset.store(end as u32, Ordering::Release);
                Some(from_raw_parts(
                    self.data.as_ptr().offset(start as isize),
                    self.size - start,
                ))
            }
        }
    }
    #[inline]
    pub fn consume(&self, len: usize) {
        self.r_offset.fetch_add(len, Ordering::AcqRel);
    }
    #[inline]
    fn convert_offset(&self, offset: usize) -> usize {
        offset & self.mask()
    }
    #[inline]
    fn mask(&self) -> usize {
        self.size - 1
    }
}

#[cfg(test)]
mod tests {
    pub trait Len {
        fn get_len(&self) -> usize;
    }
    use std::collections::HashMap;
    struct OffsetSequenceMap<T>(HashMap<usize, T>);

    impl<T> OffsetSequenceMap<T> {
        pub fn with_capacity(cap: usize) -> Self {
            OffsetSequenceMap(HashMap::with_capacity(cap))
        }
        pub fn insert(&mut self, offset: usize, len: T) {
            self.0.insert(offset, len);
        }
    }

    impl<T> super::OffsetSequence for OffsetSequenceMap<T>
    where
        T: Len,
    {
        fn next(&self, current: usize) -> usize {
            let mut n = current;
            while let Some(len) = self.0.get(&n) {
                n += len.get_len();
            }
            n
        }
    }
    impl Len for usize {
        fn get_len(&self) -> usize {
            *self
        }
    }
    #[test]
    fn test_ring_buff() {
        let mut os = OffsetSequenceMap::with_capacity(64);

        let buf = super::MonoRingBuffer::with_capacity(32);
        let b0 = b"hello world";
        let offset0 = buf.reserve(b0.len());
        assert_eq!(offset0, 0);
        let b1 = b"this is a test";
        let offset1 = buf.reserve(b1.len());
        assert_eq!(offset1, b0.len());

        assert_eq!(buf.write(offset0, b0), true);
        os.insert(offset0, b0.len());
        assert_eq!(buf.write(offset1, b1), true);
        os.insert(offset1, b1.len());

        let fetch0 = buf.fetch(&mut os);
        assert!(fetch0.is_some());
        let fetch0 = fetch0.unwrap();
        let mut b0_1 = Vec::with_capacity(64);
        b0_1.extend_from_slice(b0);
        b0_1.extend_from_slice(b1);

        assert_eq!(fetch0, b0_1);
        buf.consume(fetch0.len());

        assert_eq!(buf.fetch(&mut os), None);

        let b2 = b"another buffer fold";
        let offset2 = buf.reserve(b2.len());

        let b3 = b"insert hole!!";
        let offset3 = buf.reserve(b3.len());
        // 先写入b3.
        assert_eq!(buf.write(offset3, b3), true);
        os.insert(offset3, b3.len());
        // b2没写完，有空洞，无法读取数据
        assert_eq!(buf.fetch(&mut os), None);

        let b4 = b"write failed";
        let offset4 = buf.reserve(b4.len());
        assert_eq!(buf.write(offset4, b4), false);

        assert_eq!(buf.write(offset2, b2), true);
        os.insert(offset2, b2.len());

        // 发生折返, 数据要有两次才能全部返回
        let b2_3_0 = buf.fetch(&mut os);
        assert!(b2_3_0.is_some());
        let b2_3_0 = b2_3_0.unwrap();
        buf.consume(b2_3_0.len());

        let b2_3_1 = buf.fetch(&mut os);
        assert!(b2_3_1.is_some());
        let b2_3_1 = b2_3_1.unwrap();
        buf.consume(b2_3_1.len());

        assert_eq!(b2_3_0.len() + b2_3_1.len(), b2.len() + b3.len());

        assert_eq!(buf.write(offset4, b4), true);
    }
}
