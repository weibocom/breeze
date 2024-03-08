use crate::BrzMalloc;

use std::alloc::{GlobalAlloc, Layout};
use std::sync::atomic::{fence, AtomicU8, AtomicUsize, Ordering::*};
unsafe impl Sync for CacheArena {}
#[repr(align(64))]
pub(crate) struct CacheArena<const CAP: usize = 8388608> {
    ptr: *mut u8,
    // 指向当前访问的chunk
    idx: AtomicU8,
    // 1. 低32位目前已经分配的内存大小（指向的位置）
    // 2. 高32位用来控制释放。是释放的数量，释放的数量到0时，说明当前的arena可以重新分配内存；
    chunks: [Chunk; 2],
}
const ALIGN: usize = 64;
impl<const CAP: usize> CacheArena<CAP> {
    pub(crate) fn new() -> Self {
        let cap = CAP;
        // 按页对齐，分配内存
        debug_assert!(cap.is_power_of_two() && cap & 4095 == 0);
        let layout = Layout::array::<u8>(cap).unwrap();
        let ptr = unsafe { BrzMalloc.alloc(layout) };
        let each = cap / 2;
        Self {
            ptr,
            idx: AtomicU8::new(0),
            chunks: [Chunk::new(ptr, each), Chunk::new(ptr, each)],
        }
    }
    #[inline(always)]
    const fn cap(&self) -> usize {
        CAP
    }
    #[inline]
    fn layout(&self, size: usize) -> Layout {
        unsafe { Layout::from_size_align_unchecked(size, ALIGN) }
    }
    #[inline(always)]
    const fn max_alloc(&self) -> usize {
        self.cap() / 32
    }
    #[inline]
    pub(crate) fn alloc(&self, size: usize) -> (*mut u8, usize) {
        let size = (size + ALIGN - 1) & !(ALIGN - 1);
        if size <= self.max_alloc() {
            let idx = self.idx.load(Relaxed);
            let chunk = &self.chunks[idx as usize];
            if let Some(p) = chunk.alloc(size).or_else(|| {
                let idx = 1 - idx;
                self.idx.store(idx, Relaxed);
                self.get(idx as usize).alloc(size)
            }) {
                return p;
            }
        }
        // 从堆上分配
        unsafe { (BrzMalloc.alloc(self.layout(size)), size) }
    }
    #[inline(always)]
    fn get(&self, idx: usize) -> &Chunk {
        debug_assert!(idx <= 1);
        unsafe { &self.chunks.get_unchecked(idx) }
    }
    #[inline]
    pub(crate) fn dealloc(&self, ptr: *mut u8, size: usize) {
        debug_assert!(size & (ALIGN - 1) == 0);
        if ptr >= self.ptr && ptr < unsafe { self.ptr.add(self.cap()) } {
            let idx = ((ptr as usize - self.ptr as usize) >= self.cap()) as usize;
            debug_assert!(idx <= 1);
            self.get(idx).dealloc(ptr);
        } else {
            unsafe { BrzMalloc.dealloc(ptr, self.layout(size)) };
        }
    }
}

struct Chunk {
    ptr: *mut u8,
    alloc_oft: AtomicUsize,
    cap: usize,
}

impl Chunk {
    fn new(ptr: *mut u8, cap: usize) -> Self {
        Self {
            ptr,
            cap,
            alloc_oft: AtomicUsize::new(0),
        }
    }
    // 按64字节对齐
    #[inline(always)]
    fn alloc(&self, size: usize) -> Option<(*mut u8, usize)> {
        debug_assert!(size & (ALIGN - 1) == 0);
        let oft = self.incr_size(size);
        if oft + size <= self.cap {
            Some((unsafe { self.ptr.add(oft) }, size))
        } else {
            self.free_one();
            None
        }
    }
    #[inline(always)]
    fn incr_size(&self, size: usize) -> usize {
        debug_assert!(size + self.cap < u32::MAX as usize);
        let v = size + (1 << 32);
        let alloc_oft = self.alloc_oft.fetch_add(v, Relaxed);
        // 低32位为分配的偏移量，高32位为分配次数
        alloc_oft as u32 as usize
    }
    // 释放的时候统一释放
    #[inline(always)]
    fn free_one(&self) {
        let alloc_oft = self.alloc_oft.fetch_sub(1 << 32, Release);
        if (alloc_oft >> 32) != 1 {
            return;
        }
        fence(Acquire);
        self.alloc_oft.store(0, Release);
    }
    #[inline(always)]
    fn dealloc(&self, ptr: *mut u8) {
        debug_assert!(ptr >= self.ptr && ptr < unsafe { self.ptr.add(self.cap) });
        self.free_one();
    }
}
// Drop
impl<const CAP: usize> Drop for CacheArena<CAP> {
    fn drop(&mut self) {
        unsafe { BrzMalloc.dealloc(self.ptr, Layout::array::<u8>(self.cap()).unwrap()) };
    }
}
