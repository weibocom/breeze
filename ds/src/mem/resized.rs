use super::{MemPolicy, RingBuffer, RingSlice};

// 支持自动扩缩容的ring buffer。
// 扩容时机：在reserve_bytes_mut时触发扩容判断。如果当前容量满，或者超过4ms时间处理过的内存未通过reset释放。
// 缩容时机： 每写入回收内存时判断，如果连续1分钟使用率小于25%，则缩容一半
pub struct ResizedRingBuffer {
    // 在resize之后，不能立即释放ringbuffer，因为有可能还有外部引用。
    // 需要在所有的processed的字节都被ack之后（通过reset_read）才能释放
    max_processed: usize,
    old: Vec<RingBuffer>,
    inner: RingBuffer,
    policy: MemPolicy,
}

use std::ops::{Deref, DerefMut};

impl Deref for ResizedRingBuffer {
    type Target = RingBuffer;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for ResizedRingBuffer {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl ResizedRingBuffer {
    pub fn from(min: usize, max: usize, init: usize) -> Self {
        assert!(min <= max && init <= max);
        let buf = RingBuffer::with_capacity(init);
        Self {
            max_processed: std::usize::MAX,
            old: Vec::new(),
            inner: buf,
            policy: MemPolicy::rx(min, max),
        }
    }
    // 从src中读取数据，直到所有数据都读取完毕。满足以下任一条件时，返回：
    // 1. 读取的字节数为0；
    // 2. 读取的字节数小于buffer长度;
    // 对于场景2，有可能还有数据未读取，需要再次调用. 主要是为了降级一次系统调用的开销
    #[inline]
    pub fn copy_from<O, R: crate::BuffRead<Out = O>>(&mut self, src: &mut R) -> O {
        loop {
            // TODO 优化，如果当前buffer已经满了，可以直接返回
            self.grow(512);
            let (out, n) = self.inner.copy_from_once(src);
            if n == 0 {
                return out;
            }
        }
    }
    // 需要写入数据时，判断是否需要扩容
    //#[inline]
    //fn as_mut_bytes(&mut self) -> &mut [u8] {
    //    self.grow(512);
    //    self.inner.as_mut_bytes()
    //}
    // 有数写入时，判断是否需要缩容
    //#[inline]
    //fn advance_write(&mut self, n: usize) {
    //    if n > 0 {
    //        self.inner.advance_write(n);
    //        self.policy.check_shrink(self.len(), self.cap());
    //    }
    //    // 判断是否需要缩容
    //}
    #[inline]
    fn resize(&mut self, cap: usize) {
        let new = self.inner.resize(cap);
        let old = std::mem::replace(&mut self.inner, new);
        self.max_processed = old.writtened();
        self.old.push(old);
    }
    #[inline]
    pub fn advance_read(&mut self, n: usize) {
        self.inner.advance_read(n);
        if self.read() >= self.max_processed {
            self.old.clear();
            self.max_processed = std::usize::MAX;
        }
    }
    // 写入数据。返回是否写入成功。
    // 当buffer无法再扩容以容纳data时，写入失败，其他写入成功
    #[inline]
    pub fn write(&mut self, data: &RingSlice) -> usize {
        self.grow(data.len());
        //self.inner.write(data)
        unsafe { self.inner.write_all(data) };
        data.len()
    }
    #[inline]
    pub fn grow(&mut self, reserve: usize) {
        let len = self.len();
        if self.policy.need_grow(len, self.cap(), reserve) {
            let new = self.policy.grow(len, self.cap(), reserve);
            self.resize(new);
        }
    }
    #[inline]
    pub fn shrink(&mut self) {
        let len = self.len();
        if self.policy.need_shrink(len, self.cap()) {
            let new = self.policy.shrink(len, self.cap());
            self.resize(new);
        }
    }
}

use std::fmt::{self, Display, Formatter};
impl Display for ResizedRingBuffer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "rrb:(inner:{}, old:{:?}) policy:{}",
            self.inner, self.old, self.policy
        )
    }
}

unsafe impl Send for ResizedRingBuffer {}
unsafe impl Sync for ResizedRingBuffer {}
