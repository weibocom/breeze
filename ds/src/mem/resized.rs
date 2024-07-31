use super::{MemPolicy, RingBuffer};

// 支持自动扩缩容的ring buffer。
// 扩容时机：在reserve_bytes_mut时触发扩容判断。如果当前容量满，或者超过4ms时间处理过的内存未通过reset释放。
// 缩容时机： 每写入回收内存时判断，如果连续1分钟使用率小于25%，则缩容一半
pub struct ResizedRingBuffer {
    // 在resize之后，不能立即释放ringbuffer，因为有可能还有外部引用。
    // 需要在所有的processed的字节都被ack之后（通过reset_read）才能释放
    inner: RingBuffer,
    policy: MemPolicy,
    dropping: Dropping, // 存储已经resize的ringbuffer，等待被释放
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
            dropping: Default::default(),
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
            self.grow(512);
            let out = self.inner.copy_from(src);
            if self.inner.available() > 0 {
                return out;
            }
            // 否则说明buffer已经满了，需要再次读取
        }
    }
    #[inline]
    fn resize(&mut self, cap: usize) {
        let new = self.inner.resize(cap);
        let old = std::mem::replace(&mut self.inner, new);
        self.dropping.push(old);
    }
    #[inline]
    pub fn advance_read(&mut self, n: usize) {
        self.policy.check_shrink(self.len(), self.cap());
        self.inner.advance_read(n);
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
        if len == 0 {
            self.dropping.clear();
        }
        if self.cap() >= 2097152 {
            log::info!(
                "need_shrink: {}, shrink {:?}",
                self.policy.need_shrink(len, self.cap()),
                self
            );
        }
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
            "buf:{:?}, dropping:{:?} policy:{}",
            self.inner, self.dropping, self.policy
        )
    }
}
impl fmt::Debug for ResizedRingBuffer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

#[derive(Default)]
struct Dropping {
    ptr: usize,
}

impl Dropping {
    fn as_ref(&self) -> &Vec<RingBuffer> {
        debug_assert!(self.ptr != 0);
        unsafe { &*(self.ptr as *mut Vec<RingBuffer>) }
    }
    fn as_mut(&mut self) -> &mut Vec<RingBuffer> {
        debug_assert!(self.ptr != 0);
        unsafe { &mut *(self.ptr as *mut Vec<RingBuffer>) }
    }
    fn push(&mut self, buf: RingBuffer) {
        if buf.writtened() == 0 || buf.writtened() == buf.read() {
            // 如果没有写入过数据，或者已经读取完毕，直接释放
            return;
        }
        if self.ptr == 0 {
            let v = Box::leak(Box::new(Vec::<RingBuffer>::new()));
            self.ptr = v as *mut _ as usize;
        }
        self.as_mut().push(buf);
    }
    fn clear(&mut self) {
        if self.ptr != 0 {
            let rbs = self.as_mut();
            if rbs.len() > 0 {
                rbs.clear();
            }
        }
    }
}
impl Drop for Dropping {
    fn drop(&mut self) {
        if self.ptr > 0 {
            let ptr = self.ptr as *mut Vec<RingBuffer>;
            let _dropped = unsafe { Box::from_raw(ptr) };
            self.ptr = 0;
        }
    }
}
impl fmt::Debug for Dropping {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if self.ptr == 0 {
            write!(f, "[]")
        } else {
            write!(f, "{:?}", self.as_ref())
        }
    }
}
