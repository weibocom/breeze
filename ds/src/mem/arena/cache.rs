use std::ptr::NonNull;
// 最多缓存32个对象
pub struct CachedArena<T, H> {
    cap: u32,
    // 0表示未使用，1表示使用中。
    bits: u32,
    cache: NonNull<T>,
    heap: H,
}

impl<T, H: super::Allocator<T>> CachedArena<T, H> {
    // 最多缓存64个对象
    pub fn with_capacity(cap: usize, heap: H) -> Self {
        let cap = cap.next_power_of_two().min(32) as u32;
        let bits = !0 << cap;
        let mut cache = std::mem::ManuallyDrop::new(Vec::with_capacity(cap as usize));
        let cache = unsafe { NonNull::new_unchecked(cache.as_mut_ptr()) };
        Self {
            bits,
            cache,
            cap,
            heap,
        }
    }
    #[inline]
    pub fn alloc(&mut self, t: T) -> NonNull<T> {
        unsafe {
            if self.bits != !0 {
                let idx = self.bits.trailing_ones() as usize;
                self.bits |= 1 << idx;
                let ptr = self.cache.as_ptr().add(idx);
                ptr.write(t);
                NonNull::new_unchecked(ptr)
            } else {
                self.heap.alloc(t)
            }
        }
    }
    #[inline]
    pub fn dealloc(&mut self, ptr: NonNull<T>) {
        unsafe {
            if ptr.as_ptr() >= self.cache.as_ptr()
                && ptr.as_ptr() < self.cache.as_ptr().add(self.cap as usize)
            {
                let idx = ptr.as_ptr().offset_from(self.cache.as_ptr()) as usize;
                self.bits &= !(1 << idx);
                std::ptr::drop_in_place(ptr.as_ptr());
            } else {
                self.heap.dealloc(ptr);
            }
        }
    }
}

impl<T, H> Drop for CachedArena<T, H> {
    fn drop(&mut self) {
        assert_eq!(self.bits, !0 << self.cap, "arena is not empty");
        unsafe {
            let _cache = Vec::from_raw_parts(self.cache.as_ptr(), 0, self.cap as usize);
        }
    }
}
