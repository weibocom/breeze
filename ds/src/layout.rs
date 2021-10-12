#[cfg(feature = "single-thread")]
pub struct CacheAligned<T: Sized>(pub T);
#[cfg(feature = "single-thread")]
impl<T> CacheAligned<T> {
    pub fn new(t: T) -> Self {
        Self(t)
    }
}

#[cfg(not(feature = "single-thread"))]
pub type CacheAligned<T> = cache_line_size::CacheAligned<T>;
