use std::sync::{
    atomic::{AtomicUsize, Ordering::*},
    Arc,
};

use crate::{ResizedRingBuffer, RingSlice};

pub trait BuffRead {
    type Out;
    fn read(&mut self, b: &mut [u8]) -> (usize, Self::Out);
}

#[derive(Debug)]
pub struct GuardedBuffer {
    inner: ResizedRingBuffer,
    // 已取走未释放的位置 read <= taken <= write，释放后才会真正读走ResizedRingBuffer
    taken: usize,
    num_taken: usize,               // 已经取走的数量
    num_released: Arc<AtomicUsize>, // 已经释放的数量
}

impl GuardedBuffer {
    pub fn new(min: usize, max: usize, init: usize) -> Self {
        Self {
            inner: ResizedRingBuffer::from(min, max, init),
            taken: 0,
            num_taken: 0,
            num_released: Arc::new(AtomicUsize::new(0)),
        }
    }
    #[inline]
    pub fn write<R, O>(&mut self, r: &mut R) -> O
    where
        R: BuffRead<Out = O>,
    {
        self.gc();
        self.inner.copy_from(r)
    }
    #[inline]
    pub fn read(&self) -> RingSlice {
        self.inner
            .slice(self.taken, self.inner.writtened() - self.taken)
    }
    #[inline]
    pub fn take(&mut self, n: usize) -> MemGuard {
        assert!(n > 0);
        assert!(self.taken + n <= self.writtened());
        let data = self.inner.slice(self.taken, n);
        self.taken += n;
        self.num_taken += 1;
        let guard = Guard::new(self.num_released.clone());
        MemGuard::new(data, guard)
    }
    #[inline]
    pub fn gc(&mut self) {
        let num_released = self.num_released.load(Relaxed);
        // 所有的字节都已经taken
        // 所有taken走的数量都已经释放
        if num_released >= self.num_taken {
            self.inner.advance_read(self.pending());
        }
    }
    // 已经take但不能释放的字节数量。
    #[inline]
    pub fn pending(&self) -> usize {
        self.taken - self.inner.read()
    }
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len() - self.pending()
    }
}
use std::fmt::{self, Display, Formatter};
impl Display for GuardedBuffer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

pub struct MemGuard {
    mem: RingSlice,
    guard: Option<Guard>, //  当前guard是否拥有mem。如果拥有，则在drop时需要手工销毁内存
}

impl Deref for MemGuard {
    type Target = RingSlice;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.mem
    }
}
impl DerefMut for MemGuard {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.mem
    }
}

impl MemGuard {
    #[inline]
    fn new(data: RingSlice, guard: Guard) -> Self {
        assert_ne!(data.len(), 0);
        Self {
            mem: data,
            guard: Some(guard),
        }
    }
    #[inline]
    pub fn from_vec(data: Vec<u8>) -> Self {
        debug_assert_ne!(data.len(), 0);
        let data = std::mem::ManuallyDrop::new(data);
        let mem: RingSlice = RingSlice::from_vec(&*data);
        Self { mem, guard: None }
    }
    #[inline]
    pub fn empty() -> Self {
        let mem: RingSlice = RingSlice::empty();
        Self { mem, guard: None }
    }
}
impl Drop for MemGuard {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            if self.guard.is_none() {
                debug_assert!(self.mem.cap() >= self.mem.len());
                let _v = Vec::from_raw_parts(self.mem.ptr(), 0, self.mem.cap());
            }
        }
    }
}

use std::ops::{Deref, DerefMut};

impl Deref for GuardedBuffer {
    type Target = ResizedRingBuffer;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for GuardedBuffer {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
impl Display for MemGuard {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "data:{}  guarded:{:?}", self.mem, self.guard)
    }
}
impl fmt::Debug for MemGuard {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "data:{:?}  guarded:{:?}", self.mem, self.guard)
    }
}
impl Drop for GuardedBuffer {
    #[inline]
    fn drop(&mut self) {
        // 如果guards不为0，说明MemGuard未释放，当前buffer销毁后，会导致MemGuard指向内存错误。
        assert_eq!(self.pending(), 0, "mem leaked:{}", self);
    }
}

struct Guard {
    released: Arc<AtomicUsize>,
}
impl Guard {
    #[inline(always)]
    fn new(released: Arc<AtomicUsize>) -> Self {
        Self { released }
    }
}
impl Drop for Guard {
    #[inline]
    fn drop(&mut self) {
        self.released.fetch_add(1, Relaxed);
    }
}

impl fmt::Debug for Guard {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.released.load(Acquire))
    }
}
