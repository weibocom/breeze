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
    pub fn new() -> Self {
        Self {
            old: Vec::new(),
            max_processed: std::usize::MAX,
            inner: RingBuffer::with_capacity(32 * 1024),
        }
    }
    pub fn resize(&mut self) -> bool {
        let cap = self.inner.cap() * 2;
        // 8MB对于在线业务的一次请求，是一个足够大的值。
        if cap >= 8 * 1024 * 1024 {
            log::debug!("overflow. {}", cap);
            return false;
        }
        log::info!("ringbuffer: resize buffer to {}", cap);
        let new = self.inner.resize(cap);
        let old = std::mem::replace(&mut self.inner, new);
        self.max_processed = old.processed();
        self.old.push(old);
        true
    }
    #[inline(always)]
    pub fn reset_read(&mut self, read: usize) {
        self.inner.reset_read(read);
        if read >= self.max_processed {
            for buff in self.old.iter() {
                log::info!(
                    "buffer released. old: ({},{},{}). new: ({},{},{})",
                    buff.read(),
                    buff.processed(),
                    buff.writtened(),
                    read,
                    self.inner.processed(),
                    self.writtened(),
                );
            }
            self.old.clear();
            self.max_processed = std::usize::MAX;
        }
    }
}
