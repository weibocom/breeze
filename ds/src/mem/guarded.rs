use std::sync::atomic::{AtomicU32, Ordering};

use crate::{PinnedQueue, ResizedRingBuffer, RingSlice};

pub trait BuffRead {
    type Out;
    fn read(&mut self, b: &mut [u8]) -> (usize, Self::Out);
}

pub struct GuardedBuffer {
    inner: ResizedRingBuffer,
    // 已取走未释放的位置 read <= taken <= write，释放后才会真正读走ResizedRingBuffer
    taken: usize,
    guards: PinnedQueue<AtomicU32>,
}

impl GuardedBuffer {
    pub fn new(min: usize, max: usize, init: usize) -> Self {
        Self {
            inner: ResizedRingBuffer::from(min, max, init),
            guards: PinnedQueue::new(),
            taken: 0,
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
        debug_assert!(n > 0);
        debug_assert!(self.taken + n <= self.writtened());
        let guard = unsafe { self.guards.push_back_mut() };
        *guard.get_mut() = 0;
        let data = self.inner.slice(self.taken, n);
        self.taken += n;
        let ptr = guard as *const AtomicU32;
        MemGuard::new(data, ptr)
    }
    #[inline]
    pub fn gc(&mut self) {
        while let Some(guard) = self.guards.front_mut() {
            let guard = guard.load(Ordering::Acquire);
            if guard == 0 {
                break;
            }
            unsafe { self.guards.forget_front() };
            self.inner.advance_read(guard as usize);
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
        write!(
            f,
            "taken:{} {} guarded:{}",
            self.taken, self.inner, self.guards
        )
    }
}
impl fmt::Debug for GuardedBuffer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "taken:{} {} guarded:{}",
            self.taken, self.inner, self.guards
        )
    }
}

pub struct MemGuard {
    mem: RingSlice,
    guard: *const AtomicU32, //  当前guard是否拥有mem。如果拥有，则在drop时需要手工销毁内存
}

impl MemGuard {
    #[inline]
    fn new(data: RingSlice, guard: *const AtomicU32) -> Self {
        debug_assert!(!guard.is_null());
        debug_assert_ne!(data.len(), 0);
        unsafe { debug_assert_eq!((&*guard).load(Ordering::Acquire), 0) };
        Self { mem: data, guard }
    }
    #[inline]
    pub fn from_vec(data: Vec<u8>) -> Self {
        debug_assert_ne!(data.len(), 0);
        debug_assert!(data.capacity() < u32::MAX as usize);
        let mem: RingSlice = RingSlice::from_vec(&data);
        let _ = std::mem::ManuallyDrop::new(data);
        let guard = 0 as *const _;
        Self { mem, guard }
    }

    #[inline]
    pub fn data(&self) -> &RingSlice {
        &self.mem
    }
    #[inline]
    pub fn data_mut(&mut self) -> &mut RingSlice {
        &mut self.mem
    }
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.mem.len()
    }
}
impl Drop for MemGuard {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            if self.guard.is_null() {
                // 通过from_vec创建的, 需要释放vec内存
                debug_assert!(self.mem.cap() >= self.mem.len());
                let _v = Vec::from_raw_parts(self.mem.ptr(), 0, self.mem.cap());
            } else {
                // 通过take创建的，需要释放guard
                debug_assert_eq!((&*self.guard).load(Ordering::Acquire), 0);
                (&*self.guard).store(self.mem.len() as u32, Ordering::Release);
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
        write!(f, "data:{}  guarded:{}", self.mem, !self.guard.is_null())
    }
}
impl fmt::Debug for MemGuard {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "data:{:?}  guarded:{}", self.mem, !self.guard.is_null())
    }
}
impl Drop for GuardedBuffer {
    #[inline]
    fn drop(&mut self) {
        // 如果guards不为0，说明MemGuard未释放，当前buffer销毁后，会导致MemGuard指向内存错误。
        assert_eq!(self.guards.len(), 0, "guarded buffer dropped:{}", self);
    }
}
