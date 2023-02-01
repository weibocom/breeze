use std::{
    fmt::{Debug, Formatter},
    mem::MaybeUninit,
    ptr::NonNull,
    sync::atomic::{AtomicUsize, Ordering::*},
};

// 当对象生命周期非常短时，可以使用 Arena 来分配内存，避免频繁的内存分配和释放。
// 一共会缓存ITEM_NUM个未初始化的对象。分为两个chunk。
// 释放时，按chunk来释放
pub struct Ephemera<T, const ITEM_NUM: usize> {
    data: [MaybeUninit<T>; ITEM_NUM],
    chunk0: Chunk,
    chunk1: Chunk,
}

impl<T, const ITEM_NUM: usize> Ephemera<T, ITEM_NUM> {
    const LEN0: usize = ITEM_NUM / 2;
    const LEN1: usize = ITEM_NUM - Self::LEN0;
    pub const fn new() -> Self {
        assert!(ITEM_NUM > 1);
        assert!(std::mem::size_of::<T>() > 0);
        unsafe {
            let data = MaybeUninit::<[MaybeUninit<T>; ITEM_NUM]>::uninit().assume_init();
            let chunk0 = Chunk::new(0, Self::LEN0);
            let chunk1 = Chunk::new(Self::LEN0, Self::LEN1);
            Self {
                data,
                chunk0,
                chunk1,
            }
        }
    }
    #[inline]
    unsafe fn alloc_by_idx(&self, idx: usize, t: T) -> NonNull<T> {
        super::super::CACHE_ALLOC_NUM.fetch_add(1, Relaxed);
        let ptr = self.ptr().add(idx);
        ptr.write(t);
        NonNull::new_unchecked(ptr)
    }
    #[inline]
    pub fn alloc(&self, t: T) -> NonNull<T> {
        unsafe {
            self.chunk0.try_clear();
            self.chunk1.try_clear();
            if let Some(idx) = self.chunk0.reserve().or_else(|| self.chunk1.reserve()) {
                self.alloc_by_idx(idx, t)
            } else {
                let ptr = Box::into_raw(Box::new(t));
                NonNull::new_unchecked(ptr)
            }
        }
    }
    #[inline(always)]
    fn ptr(&self) -> *mut T {
        self.data.as_ptr() as *const _ as *mut _
    }
    #[inline]
    pub fn dealloc(&self, t: NonNull<T>) {
        unsafe {
            if t.as_ptr() >= self.ptr() && t.as_ptr() < self.ptr().add(ITEM_NUM) {
                std::ptr::drop_in_place(t.as_ptr());
                if t.as_ptr() < self.ptr().add(Self::LEN0) {
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

impl<T, const ITEM_NUM: usize> Drop for Ephemera<T, ITEM_NUM> {
    fn drop(&mut self) {}
}
impl<T, const ITEM_NUM: usize> Default for Ephemera<T, ITEM_NUM> {
    fn default() -> Self {
        Self::new()
    }
}
impl<T, const ITEM_NUM: usize> Debug for Ephemera<T, ITEM_NUM> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "chunk0:{:?} chunk1:{:?}", self.chunk0, self.chunk1)
    }
}

unsafe impl<T, const ITEM_NUM: usize> Sync for Ephemera<T, ITEM_NUM> {}

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
        assert!(_old < self.len);
    }
    #[inline]
    fn try_clear(&self) {
        if self.free.load(Acquire) == self.len {
            self.free.store(0, Release);
            self.idx.store(0, Release);
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
