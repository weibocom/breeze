use super::RingBuffer;
pub struct ResizedRingBuffer {
    // 在resize之后，不能立即释放ringbuffer，因为有可能还有外部引用。
    // 需要在所有的processed的字节都被ack之后（通过reset_read）才能释放
    max_processed: usize,
    old: Vec<RingBuffer>,
    inner: RingBuffer,
}

use std::ops::{Deref, DerefMut};

impl Deref for ResizedRingBuffer {
    type Target = RingBuffer;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for ResizedRingBuffer {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl ResizedRingBuffer {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            old: Vec::new(),
            max_processed: std::usize::MAX,
            inner: RingBuffer::with_capacity(cap),
        }
    }
    // 每次扩容两倍
    #[inline]
    pub fn scaleup(&mut self) -> bool {
        let new = self.inner.cap() * 2;
        // 8MB对于在线业务的一次请求，是一个足够大的值。
        if new >= 8 * 1024 * 1024 {
            return false;
        }
        self.resize(new);
        true
    }
    #[inline]
    pub(crate) fn resize(&mut self, cap: usize) {
        let new = self.inner.resize(cap);
        let old = std::mem::replace(&mut self.inner, new);
        self.max_processed = old.processed();
        self.old.push(old);
    }
    #[inline(always)]
    pub fn reset_read(&mut self, read: usize) {
        self.inner.reset_read(read);
        if read >= self.max_processed {
            self.old.clear();
            self.max_processed = std::usize::MAX;
        }
    }
    // 所有数据都已经处理完成，并且数据已经填充完成
    #[inline]
    pub fn full(&self) -> bool {
        self.read() == self.processed() && self.writtened() - self.read() == self.cap()
    }
}

use std::fmt::{self, Display, Formatter};
impl Display for ResizedRingBuffer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "rrb:(inner:{}, old:{:?})", self.inner, self.old)
    }
}
