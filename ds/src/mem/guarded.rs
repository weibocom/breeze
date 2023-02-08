use crate::ResizedRingBuffer;
use std::sync::atomic::{AtomicU32, Ordering};

use super::RingSlice;
use crate::PinnedQueue;

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
        loop {
            let b = self.inner.as_mut_bytes();
            let cap = b.len();
            let (n, out) = r.read(b);
            self.inner.advance_write(n);
            if cap > n || n == 0 {
                return out;
            }
        }
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
    //#[inline]
    //pub fn update(&mut self, idx: usize, val: u8) {
    //    let oft = self.offset(idx);
    //    self.inner.update(oft, val);
    //}
    //#[inline]
    //pub fn at(&self, idx: usize) -> u8 {
    //    self.inner.at(self.offset(idx))
    //}
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len() - self.pending()
    }
    //#[inline]
    //fn offset(&self, oft: usize) -> usize {
    //    self.pending() + oft
    //}
    //#[inline]
    //pub fn raw(&self) -> &[u8] {
    //    self.inner.raw()
    //}
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

pub struct MemGuard {
    mem: RingSlice,
    guard: *const AtomicU32, //  当前guard是否拥有mem。如果拥有，则在drop时需要手工销毁内存
    cap: usize,              // from vec时，cap存储了Vec::capacity()用于释放内存
}

impl MemGuard {
    #[inline]
    fn new(data: RingSlice, guard: *const AtomicU32) -> Self {
        assert!(!guard.is_null());
        assert_ne!(data.len(), 0);
        unsafe { assert_eq!((&*guard).load(Ordering::Acquire), 0) };
        Self {
            mem: data,
            guard,
            cap: 0,
        }
    }
    #[inline]
    pub fn from_vec(data: Vec<u8>) -> Self {
        let mem: RingSlice = data.as_slice().into();
        //assert_eq!(data.capacity(), mem.len());
        assert_ne!(data.len(), 0);
        let cap = data.capacity();
        let _ = std::mem::ManuallyDrop::new(data);
        let guard = 0 as *const _;
        Self { mem, guard, cap }
    }

    #[inline]
    pub fn data(&self) -> &RingSlice {
        &self.mem
    }
    #[inline]
    pub fn data_mut(&mut self) -> &mut RingSlice {
        &mut self.mem
    }
    #[inline]
    pub fn len(&self) -> usize {
        self.mem.len()
    }
    //#[inline]
    //pub fn read(&self, oft: usize) -> &[u8] {
    //    self.mem.read(oft)
    //}
}
impl Drop for MemGuard {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            if self.guard.is_null() {
                assert!(self.cap >= self.mem.len());
                let _v = Vec::from_raw_parts(self.mem.ptr(), 0, self.cap);
            } else {
                assert_eq!((&*self.guard).load(Ordering::Acquire), 0);
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
impl Drop for GuardedBuffer {
    #[inline]
    fn drop(&mut self) {
        // 如果guards不为0，说明MemGuard未释放，当前buffer销毁后，会导致MemGuard指向内存错误。
        assert_eq!(self.guards.len(), 0, "guarded buffer dropped:{}", self);
    }
}
