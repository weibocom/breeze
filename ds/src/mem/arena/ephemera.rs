use std::{
    fmt::{Debug, Formatter},
    ptr::NonNull,
    sync::atomic::{AtomicUsize, Ordering::*},
};

// 当对象生命周期非常短时，可以使用 Arena 来分配内存，避免频繁的内存分配和释放。
// 一共会缓存ITEM_NUM个未初始化的对象。分为两个chunk。
// 释放时，按chunk来释放
pub struct Ephemera<T> {
    data: *mut T,
    end: *mut T,
    chunk0: Chunk,
    chunk1: Chunk,
}

impl<T> Ephemera<T> {
    pub fn with_cache(cache: usize) -> Self {
        let cap = (cache.max(4) + 3) & !3;
        assert!(std::mem::size_of::<T>() > 0);
        let data =
            unsafe { std::alloc::alloc(std::alloc::Layout::array::<T>(cap).unwrap()) as *mut T };
        let end = unsafe { data.add(cap) };
        let len0 = cap / 2;
        let len1 = cap - len0;
        let chunk0 = Chunk::new(0, len0);
        let chunk1 = Chunk::new(len0, len1);
        Self {
            data,
            end,
            chunk0,
            chunk1,
        }
    }
    #[inline]
    pub fn alloc(&self, t: T) -> NonNull<T> {
        unsafe {
            self.chunk0.try_clear();
            self.chunk1.try_clear();
            if let Some(idx) = self.chunk0.reserve().or_else(|| self.chunk1.reserve()) {
                super::super::CACHE_ALLOC_NUM.fetch_add(1, Relaxed);
                let ptr = self.ptr().add(idx);
                ptr.write(t);
                NonNull::new_unchecked(ptr)
            } else {
                super::super::CACHE_MISS_ALLOC_NUM.fetch_add(1, Relaxed);
                let ptr = Box::into_raw(Box::new(t));
                NonNull::new_unchecked(ptr)
            }
        }
    }
    #[inline(always)]
    fn ptr(&self) -> *mut T {
        self.data
    }
    #[inline]
    pub fn dealloc(&self, t: NonNull<T>) {
        unsafe {
            if t.as_ptr() >= self.ptr() && t.as_ptr() < self.end {
                std::ptr::drop_in_place(t.as_ptr());
                if t.as_ptr() < self.ptr().add(self.chunk0.len) {
                    self.chunk0.release();
                } else {
                    self.chunk1.release();
                }
            } else {
                let _dropped = Box::from_raw(t.as_ptr());
            }
        }
    }
}
pub static HEAP: AtomicUsize = AtomicUsize::new(0);

impl<T> Drop for Ephemera<T> {
    fn drop(&mut self) {
        let cap = self.chunk0.len + self.chunk1.len;
        unsafe {
            std::alloc::dealloc(
                self.data as *mut u8,
                std::alloc::Layout::array::<T>(cap).unwrap(),
            );
        }
    }
}
impl<T> Debug for Ephemera<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "chunk0:{:?} chunk1:{:?}", self.chunk0, self.chunk1)
    }
}

unsafe impl<T> Sync for Ephemera<T> {}

struct Chunk {
    oft: usize,
    len: usize,
    idx: AtomicUsize,
    free: AtomicUsize,
}

impl Chunk {
    const fn new(oft: usize, len: usize) -> Self {
        Self {
            oft,
            len,
            idx: AtomicUsize::new(0),
            free: AtomicUsize::new(0),
        }
    }
    #[inline(always)]
    fn reserve(&self) -> Option<usize> {
        let pos = self.idx.fetch_add(1, AcqRel);
        if pos < self.len {
            Some(pos + self.oft)
        } else {
            None
        }
    }
    #[inline(always)]
    fn release(&self) {
        let _old = self.free.fetch_add(1, AcqRel);
        debug_assert!(_old < self.len);
    }
    #[inline]
    fn try_clear(&self) {
        if self.free.load(Acquire) == self.len {
            if let Ok(_) = self.free.compare_exchange(self.len, 0, AcqRel, Acquire) {
                self.idx.store(0, Release);
            }
        }
    }
}
impl Debug for Chunk {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "len:{} idx:{} free:{}",
            self.len,
            self.idx.load(Acquire),
            self.free.load(Acquire)
        )
    }
}
impl Drop for Chunk {
    fn drop(&mut self) {
        let free = self.free.load(Acquire);
        let alloc = self.idx.load(Acquire);
        // 如果有内存泄漏，这里会panic
        // 如果Chunk已经分配完，那么free一定等于len
        // 如果Chunk还有剩余，那么free == alloc
        assert!(free == alloc || free == self.len, "mem leak:{:?}", self);
    }
}
