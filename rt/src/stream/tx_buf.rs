use metrics::base::*;

use super::MemPolicy;
use std::{
    mem::ManuallyDrop,
    ptr::{copy_nonoverlapping as copy, NonNull},
};
pub struct TxBuffer {
    read: usize,
    write: usize,
    pub(super) cap: usize,
    data: NonNull<u8>,
    policy: MemPolicy,
}

impl TxBuffer {
    #[inline]
    pub fn new() -> Self {
        Self {
            read: 0,
            write: 0,
            cap: 0,
            data: NonNull::dangling(),
            policy: MemPolicy::tx(),
        }
    }
    #[inline]
    pub fn cap(&self) -> usize {
        self.cap
    }
    #[inline]
    pub(super) fn avail(&self) -> bool {
        self.write > self.read
    }
    #[inline]
    pub fn len(&self) -> usize {
        self.write
    }
    #[inline]
    pub fn data(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self.data.as_ptr().offset(self.read as isize),
                self.write - self.read,
            )
        }
    }
    #[inline]
    pub fn take(&mut self, n: usize) -> bool {
        self.read += n;
        if self.read == self.write {
            self.policy.check_shrink(self.len(), self.cap);
            self.read = 0;
            self.write = 0;
            true
        } else {
            false
        }
    }

    #[inline]
    pub fn write(&mut self, data: &[u8]) {
        if self.policy.need_grow(self.len(), self.cap, data.len()) {
            let new = self.policy.grow(self.len(), self.cap, data.len());
            self.resize(new);
        }
        assert!(self.cap - self.write >= data.len());
        // copy data to buffer
        unsafe {
            copy(
                data.as_ptr(),
                self.data.as_ptr().offset(self.write as isize),
                data.len(),
            );
        }
        self.write += data.len();
        P_W_CACHE.incr();
    }
    #[inline]
    pub(super) fn shrink(&mut self) {
        if self.policy.need_shrink(self.len(), self.cap) {
            let new = self.policy.shrink(self.len(), self.cap);
            self.resize(new);
        }
    }
    #[inline]
    fn resize(&mut self, new: usize) {
        let data = if new > 0 {
            ds::BUF_TX.incr_by(new);
            let mut data = ManuallyDrop::new(Vec::<u8>::with_capacity(new));
            let data = unsafe { NonNull::new_unchecked(data.as_mut_ptr()) };
            // 从self.read..self.write的数据复制过来
            if self.write > self.read {
                unsafe {
                    copy(
                        self.data.as_ptr().offset(self.read as isize),
                        data.as_ptr().offset(self.read as isize),
                        self.write - self.read,
                    );
                }
            }
            data
        } else {
            NonNull::dangling()
        };
        // 释放老的data
        if self.cap > 0 {
            let _old = unsafe { Vec::<u8>::from_raw_parts(self.data.as_ptr(), 0, self.cap) };
            ds::BUF_TX.decr_by(self.cap);
        }
        self.data = data;
        self.cap = new;
    }
}

impl Drop for TxBuffer {
    #[inline]
    fn drop(&mut self) {
        self.resize(0);
    }
}

unsafe impl Send for TxBuffer {}
unsafe impl Sync for TxBuffer {}
