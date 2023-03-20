//use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

//const CONFIG_UPDATED_KEY: &str = "__config__";
#[derive(Clone)]
pub struct Notify {
    // 第一个元素是config更新的标记
    inner: Vec<(String, Arc<AtomicBool>)>,
}

impl Notify {
    pub fn contains_key(&self, key: &str) -> bool {
        for (k, _) in self.inner.iter() {
            if k == key {
                return true;
            }
        }
        false
    }
    pub fn insert(&mut self, key: String, value: Arc<AtomicBool>) {
        debug_assert!(!self.contains_key(&key));
        self.inner.push((key, value));
    }
    pub fn notify(&mut self) {
        self.inner
            .first_mut()
            .expect("config updated key not found")
            .1
            .store(true, Ordering::Release);
    }
    // 有一个等待的，就返回true
    pub fn waiting(&self) -> bool {
        for (_, updated) in self.inner.iter() {
            if updated.load(Ordering::Acquire) {
                return true;
            }
        }
        false
    }
    // 清除所有的等待状态
    pub fn clear(&self) {
        for (_, updated) in self.inner.iter() {
            if updated.load(Ordering::Acquire) {
                updated.store(false, Ordering::Release);
            }
        }
    }
}

impl Default for Notify {
    fn default() -> Self {
        let mut inner = Vec::with_capacity(32);
        inner.push((String::new(), Arc::new(AtomicBool::new(false))));
        Self { inner }
    }
}
