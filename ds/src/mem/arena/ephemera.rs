use std::{
    alloc::{alloc, Layout},
    fmt::{Debug, Formatter},
    ptr::NonNull,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering::*},
};

// 当对象生命周期非常短时，可以使用 Arena 来分配内存，避免频繁的内存分配和释放。
// 一共会缓存ITEM_NUM个未初始化的对象。分为两个chunk。
// 释放时，按chunk来释放
#[repr(align(64))]
pub struct Ephemera<T> {
    data: *mut T,
    end: *mut T,
    idx: AtomicUsize,
    chunks: [Chunk; 2],
}

impl<T> Ephemera<T> {
    // cache会向power_of_two取整
    pub fn with_cache(cache: usize) -> Self {
        // 最小4个，最多100万个对象。
        let cap = cache.max(4).min(1 << 20).next_power_of_two();
        assert!(std::mem::size_of::<T>() > 0);
        let data = unsafe { alloc(Layout::array::<T>(cap).unwrap()) as *mut T };
        let end = unsafe { data.add(cap) };
        let len0 = cap / 2;
        let len1 = cap - len0;
        let chunk0 = Chunk::new(0, len0 as u32);
        let chunk1 = Chunk::new(len0 as u32, len1 as u32);
        Self {
            data,
            end,
            idx: 0.into(),
            chunks: [chunk0, chunk1],
        }
    }
    #[inline]
    pub fn alloc(&self, t: T) -> NonNull<T> {
        unsafe {
            let idx = self.idx.load(Acquire);
            if let Some(oft) = self.chunks[idx].reserve().or_else(|| {
                // store时的并发问题带来的副作用：多调用了几次store。别有额外的影响, 不需要cas
                self.idx.store(1 - idx, Release);
                self.chunks[1 - idx].reserve()
            }) {
                super::super::CACHE_ALLOC_NUM.fetch_add(1, Relaxed);
                let ptr = self.ptr().add(oft as usize);
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
                if t.as_ptr() < self.ptr().add(self.chunks[0].len as usize) {
                    self.chunks[0].release();
                } else {
                    self.chunks[1].release();
                }
            } else {
                let _dropped = Box::from_raw(t.as_ptr());
            }
        }
    }
}

impl<T> Drop for Ephemera<T> {
    fn drop(&mut self) {
        let cap = (self.chunks[0].len + self.chunks[1].len) as usize;
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
        write!(f, "chunk0:{:?} chunk1:{:?}", self.chunks[0], self.chunks[1])
    }
}

unsafe impl<T> Sync for Ephemera<T> {}

struct Chunk {
    oft: u32,
    len: u32,
    // 高32位是当前分配指向的索引位置
    // 低32位是已经分配的数量
    // idx 可能大于 len，表示已经分配完了。
    // idx 一定大于 free
    idx_free: AtomicU64,
}

impl Chunk {
    const fn new(oft: u32, len: u32) -> Self {
        Self {
            oft,
            len,
            idx_free: AtomicU64::new(0),
        }
    }
    #[inline(always)]
    fn reserve(&self) -> Option<u32> {
        let pos = (self.idx_free.fetch_add(1 << 32, AcqRel) >> 32) as u32;
        if pos < self.len {
            Some(pos + self.oft)
        } else {
            assert!(pos < u32::MAX, "overflow:{} => {:?}", pos, self);
            None
        }
    }
    #[inline(always)]
    fn release(&self) {
        let old = self.idx_free.fetch_add(1, AcqRel);
        if old as u32 == self.len as u32 - 1 {
            // idx >= len
            assert!((old >> 32) >= self.len as u64, "bug:{} => {:?}", old, self);
            // 当前chunk已经分配完成，可以清理
            // 不需要cas，因为只有一个线程会执行到这里
            self.idx_free.store(0, Release);
        }
    }
}
impl Debug for Chunk {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let idx_free = self.idx_free.load(Acquire);
        let idx = (idx_free >> 32) as usize;
        let free = idx_free as u32;
        f.debug_struct("Chunk")
            .field("oft", &self.oft)
            .field("len", &self.len)
            .field("idx", &idx)
            .field("free", &free)
            .finish()

        //write!(f, "len:{} idx:{} free:{}", self.len, idx, free,)
    }
}
impl Drop for Chunk {
    fn drop(&mut self) {
        let idx_free = self.idx_free.load(Acquire);
        let idx = (idx_free >> 32) as u32;
        let free = idx_free as u32;
        // 如果有内存泄漏，这里会panic
        // 如果Chunk已经分配完，那么free一定等于len
        // 如果Chunk还有剩余，那么free == alloc
        assert!(free == idx || free == self.len, "mem leak:{:?}", self);
    }
}
