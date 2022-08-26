use super::{MemPolicy, RingBuffer, RingSlice};

type Callback = Box<dyn FnMut(usize, isize)>;
// 支持自动扩缩容的ring buffer。
// 扩容时机：在reserve_bytes_mut时触发扩容判断。如果当前容量满，或者超过4ms时间处理过的内存未通过reset释放。
// 缩容时机： 每写入回收内存时判断，如果连续1分钟使用率小于25%，则缩容一半
pub struct ResizedRingBuffer {
    // 在resize之后，不能立即释放ringbuffer，因为有可能还有外部引用。
    // 需要在所有的processed的字节都被ack之后（通过reset_read）才能释放
    max_processed: usize,
    old: Vec<RingBuffer>,
    inner: RingBuffer,
    // 下面的用来做扩缩容判断
    min: u32,
    max: u32,
    on_change: Callback,
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
    pub fn from<F: FnMut(usize, isize) + 'static>(
        min: usize,
        max: usize,
        init: usize,
        mut cb: F,
    ) -> Self {
        assert!(min <= init && init <= max);
        cb(0, init as isize);
        Self {
            max_processed: std::usize::MAX,
            old: Vec::new(),
            inner: RingBuffer::with_capacity(init),
            min: min as u32,
            max: max as u32,
            on_change: Box::new(cb),
            policy: MemPolicy::rx(),
        }
    }
    // 需要写入数据时，判断是否需要扩容
    #[inline]
    pub fn as_mut_bytes(&mut self) -> &mut [u8] {
        self.grow(512);
        self.inner.as_mut_bytes()
    }
    // 有数写入时，判断是否需要缩容
    #[inline]
    pub fn advance_write(&mut self, n: usize) {
        self.inner.advance_write(n);
        // 判断是否需要缩容
        if self.policy.need_shrink(self.len(), self.cap()) {
            let new = self.policy.shrink(self.len(), self.cap());
            self.resize(new);
        }
    }
    #[inline]
    pub fn resize(&mut self, cap: usize) {
        assert!(cap <= self.max as usize);
        assert!(cap >= self.min as usize);
        let new = self.inner.resize(cap);
        let old = std::mem::replace(&mut self.inner, new);
        self.max_processed = old.writtened();
        self.on_change(old.cap(), self.cap() as isize);
        self.old.push(old);
    }
    #[inline]
    pub fn advance_read(&mut self, n: usize) {
        self.inner.advance_read(n);
        if self.read() >= self.max_processed {
            let delta = self.old.iter().fold(0usize, |mut s, b| {
                s += b.cap();
                s
            });
            self.on_change(self.cap(), delta as isize * -1);
            self.old.clear();
            self.max_processed = std::usize::MAX;
        }
    }
    #[inline]
    fn on_change(&mut self, cap: usize, delta: isize) {
        (*self.on_change)(cap, delta)
    }
    // 写入数据。返回是否写入成功。
    // 当buffer无法再扩容以容纳data时，写入失败，其他写入成功
    #[inline]
    pub fn write(&mut self, data: &RingSlice) -> usize {
        self.grow(data.len());
        self.inner.write(data)
    }
    #[inline(always)]
    fn grow(&mut self, reserve: usize) {
        if self.policy.need_grow(self.len(), self.cap(), reserve) {
            let new = self.policy.grow(self.len(), self.cap(), reserve);
            self.resize(new);
        }
    }
}

use std::fmt::{self, Display, Formatter};
impl Display for ResizedRingBuffer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "rrb:(inner:{}, old:{:?})", self.inner, self.old)
    }
}

impl Drop for ResizedRingBuffer {
    fn drop(&mut self) {
        let cap = self.cap();
        let delta = self.old.iter().fold(cap, |mut s, b| {
            s += b.cap();
            s
        });
        self.on_change(cap, delta as isize * -1);
    }
}
unsafe impl Send for ResizedRingBuffer {}
unsafe impl Sync for ResizedRingBuffer {}
