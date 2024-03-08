use std::ptr::copy_nonoverlapping;

use crate::{arena::CacheArena, Buffer};

// 用来分配快速释放的Vec<u8>
// 1. cap大小固定, 不支持动态扩展；
// 2. 内存会优先从cache中分配，如果cache中没有足够的内存，会从堆中分配；
pub struct EphemeralVec {
    ptr: *mut u8,
    len: u32,
    cap: u32,
}
impl From<Vec<u8>> for EphemeralVec {
    #[inline]
    fn from(vec: Vec<u8>) -> Self {
        assert!(vec.capacity() < u32::MAX as usize);
        let mut v = Self::fix_cap(vec.len());
        v.write(vec.as_slice());
        v
    }
}

impl EphemeralVec {
    pub fn fix_cap(cap: usize) -> Self {
        let (ptr, cap) = VEC_CACHE_ARENA.alloc(cap);
        assert!(cap < u32::MAX as usize);
        Self {
            ptr,
            len: 0,
            cap: cap as u32,
        }
    }
    #[inline(always)]
    pub fn into_raw_parts(self) -> (*mut u8, usize, usize) {
        let me = std::mem::ManuallyDrop::new(self);
        (me.ptr, me.len as usize, me.cap as usize)
    }
    #[inline(always)]
    pub fn from_raw_parts(ptr: *mut u8, len: usize, cap: usize) -> Self {
        assert!(len <= cap && cap < u32::MAX as usize);
        Self {
            ptr,
            len: len as u32,
            cap: cap as u32,
        }
    }
    #[inline(always)]
    pub fn push(&mut self, val: u8) {
        assert!(self.len < self.cap);
        unsafe { *self.ptr.add(self.len as usize) = val };
        self.len += 1;
    }
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len as usize
    }
    #[inline(always)]
    pub fn cap(&self) -> usize {
        self.cap as usize
    }
}
// 实现Deref<[u8]>
use std::ops::{Deref, DerefMut};
impl Deref for EphemeralVec {
    type Target = [u8];
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len as usize) }
    }
}
impl DerefMut for EphemeralVec {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len as usize) }
    }
}
impl Buffer for EphemeralVec {
    #[inline(always)]
    fn write<D: AsRef<[u8]>>(&mut self, d: D) {
        let data = d.as_ref();
        assert!(self.len() + data.len() <= self.cap());
        unsafe { copy_nonoverlapping(data.as_ptr(), self.ptr.add(self.len()), data.len()) };
        self.len += data.len() as u32;
    }
}
// 实现Drop
// 当EphemeralVec被drop时，会自动释放内存
impl Drop for EphemeralVec {
    #[inline(always)]
    fn drop(&mut self) {
        VEC_CACHE_ARENA.dealloc(self.ptr, self.cap as usize);
    }
}
#[ctor::ctor]
static VEC_CACHE_ARENA: CacheArena = CacheArena::new();
