use metrics::base::*;

use super::MemPolicy;
use std::{
    mem::ManuallyDrop,
    ptr::{copy_nonoverlapping as copy, NonNull},
};
// 最大支持u32::MAX大小的buffer。
#[derive(Debug)]
pub struct TxBuffer {
    read: u32,
    write: u32,
    cap: u32,
    pub(super) enable: bool,
    data: NonNull<u8>,
    policy: MemPolicy,
}

impl TxBuffer {
    #[inline]
    pub fn new() -> Self {
        Self {
            enable: false,
            read: 0,
            write: 0,
            cap: 0,
            data: NonNull::dangling(),
            policy: MemPolicy::tx(),
        }
    }
    #[inline]
    pub fn cap(&self) -> usize {
        self.cap as usize
    }
    #[inline]
    pub(super) fn avail(&self) -> bool {
        self.write > self.read
    }
    #[inline(always)]
    pub(super) fn w_num(&self) -> usize {
        self.write as usize
    }
    #[inline]
    pub fn len(&self) -> usize {
        self.write as usize
    }
    #[inline(always)]
    fn ptr_r(&self) -> *const u8 {
        unsafe { self.data.as_ptr().offset(self.read as isize) }
    }
    #[inline(always)]
    fn ptr_w(&mut self) -> *mut u8 {
        unsafe { self.data.as_ptr().offset(self.write as isize) }
    }
    #[inline]
    pub fn data(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr_r(), (self.write - self.read) as usize) }
    }
    #[inline]
    pub fn take(&mut self, n: usize) {
        self.read += n as u32;
        if self.read == self.write {
            self.policy.check_shrink(self.len(), self.cap());
            self.read = 0;
            self.write = 0;
        }
    }

    #[inline]
    pub fn write(&mut self, data: &[u8]) {
        if self.policy.need_grow(self.len(), self.cap(), data.len()) {
            let new = self.policy.grow(self.len(), self.cap(), data.len());
            self.resize(new);
        }
        debug_assert!(self.cap() - self.write as usize >= data.len());
        // copy data to buffer
        unsafe { copy(data.as_ptr(), self.ptr_w(), data.len()) };
        self.write += data.len() as u32;
        P_W_CACHE.incr();
    }
    #[inline]
    pub(super) fn shrink(&mut self) {
        if self.policy.need_shrink(self.len(), self.cap()) {
            let new = self.policy.shrink(self.len(), self.cap());
            self.resize(new);
        }
    }
    // 1. 创建一个大小精确为new的Vec
    // 2. 从self.read..self.write的数据复制到Vec
    // 3. 释放老的Vec
    #[inline]
    fn resize(&mut self, new: usize) {
        assert!(new < u32::MAX as usize && new > 0, "{self:?}");
        ds::BUF_TX.incr_by(new);
        let mut data = ManuallyDrop::new(Vec::<u8>::with_capacity(new));
        assert_eq!(data.capacity(), new, "{self:?}");
        let data = unsafe { NonNull::new_unchecked(data.as_mut_ptr()) };
        let count = (self.write - self.read) as usize;
        unsafe { copy(self.ptr_r(), data.as_ptr(), count) };
        // 释放老的data
        self.free();
        self.data = data;
        self.read = 0;
        self.write = count as u32;
        self.cap = new as u32;
    }
    fn free(&mut self) {
        if self.cap > 0 {
            let _old = unsafe { Vec::<u8>::from_raw_parts(self.data.as_ptr(), 0, self.cap()) };
            ds::BUF_TX.decr_by(self.cap());
        }
        self.data = NonNull::dangling();
        self.cap = 0;
        self.read = 0;
        self.write = 0;
    }
}

impl Drop for TxBuffer {
    #[inline]
    fn drop(&mut self) {
        self.free();
    }
}

unsafe impl Send for TxBuffer {}
unsafe impl Sync for TxBuffer {}
