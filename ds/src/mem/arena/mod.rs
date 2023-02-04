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

pub struct Arena<T, H = Heap> {
    cache: cache::CachedArena<T, H>,
}
impl<T, H: Allocator<T>> Arena<T, H> {
    //#[inline]
    //pub fn cache(cache: usize) -> Self {
    //    Self {
    //        cache: cache::CachedArena::with_capacity(cache, Heap),
    //    }
    //}
    #[inline]
    pub fn with_cache(cache: usize, heap: H) -> Self {
        Self {
            cache: cache::CachedArena::with_capacity(cache, heap),
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
unsafe impl<T, H> Sync for Arena<T, H> {}
unsafe impl<T, H> Send for Arena<T, H> {}
