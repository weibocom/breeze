/// 提供一写多读的高性能的数据结构。
use std::sync::RwLock;
pub struct Spmc<T> {
    inner: RwLock<T>,
}

impl<T> Spmc<T> {
    pub fn from(t: T) -> Self {
        Self {
            inner: RwLock::new(t),
        }
    }
    #[inline]
    pub fn write<F: Fn(&mut T) -> O, O>(&self, w: F) -> O {
        let mut t = self.inner.write().unwrap();
        w(&mut *t)
    }
    #[inline]
    pub fn read<F: Fn(&T) -> O, O>(&self, r: F) -> O {
        let t = self.inner.read().unwrap();
        r(&*t)
    }
}
