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
    _mark: std::marker::PhantomData<T>,
}
impl<T> Arena<T> {
    #[inline]
    pub fn cache(_cache: usize) -> Self {
        Self {
            _mark: Default::default(),
        }
    }
    #[inline]
    pub fn alloc(&mut self, t: T) -> NonNull<T> {
        Heap.alloc(t)
    }
    #[inline]
    pub fn dealloc(&mut self, t: NonNull<T>) {
        Heap.dealloc(t);
    }
}
