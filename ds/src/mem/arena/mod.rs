mod ephemera;
pub use ephemera::*;

mod cache;
pub use cache::*;

use std::ptr::NonNull;
pub trait Allocator<T> {
    fn alloc(&self, t: T) -> NonNull<T>;
    fn dealloc(&self, t: NonNull<T>);
}

pub struct Heap;
impl<T> Allocator<T> for Heap {
    #[inline(always)]
    fn alloc(&self, t: T) -> NonNull<T> {
        unsafe { NonNull::new_unchecked(Box::leak(Box::new(t))) }
    }
    #[inline(always)]
    fn dealloc(&self, t: NonNull<T>) {
        unsafe {
            let _ = Box::from_raw(t.as_ptr());
        }
    }
}

pub struct Arena<T> {
    cache: cache::CachedArena<T, Heap>,
}
impl<T> Arena<T> {
    #[inline]
    pub fn cache(cache: usize) -> Self {
        Self {
            cache: cache::CachedArena::with_capacity(cache, Heap),
        }
    }
    #[inline(always)]
    pub fn alloc(&mut self, t: T) -> NonNull<T> {
        self.cache.alloc(t)
    }
    #[inline(always)]
    pub fn dealloc(&mut self, t: NonNull<T>) {
        self.cache.dealloc(t);
    }
}
unsafe impl<T> Sync for Arena<T> {}
unsafe impl<T> Send for Arena<T> {}
