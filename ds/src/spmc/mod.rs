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
        loop {
            if let Ok(mut t) = self.inner.try_write() {
                return w(&mut *t);
            }
            println!("spining");
        }
    }
    #[inline]
    pub fn read<F: Fn(&T) -> O, O>(&self, r: F) -> O {
        let t = self.inner.read().unwrap();
        r(&*t)
    }
}
