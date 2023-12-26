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
    #[inline]
    pub fn len(&self) -> usize {
        self.write as usize
    }
    #[inline]
    pub fn taked(&self) -> usize {
        self.read as usize
    }
    #[inline]
    pub fn data(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self.data.as_ptr().offset(self.read as isize),
                (self.write - self.read) as usize,
            )
        }
    }
    #[inline]
    pub fn take(&mut self, n: usize) -> bool {
        self.read += n as u32;
        if self.read == self.write {
            self.policy.check_shrink(self.len(), self.cap());
            self.read = 0;
            self.write = 0;
            true
        } else {
            false
        }
    }

    #[inline]
    pub fn write(&mut self, data: &[u8]) {
        if data.len() > 5 * 1024 * 1024 {
            println!("write data len:{}, head {:?}", data.len(), &data[0..256])
        }
        if self.policy.need_grow(self.len(), self.cap(), data.len()) {
            let new = self.policy.grow(self.len(), self.cap(), data.len());
            self.resize(new);
        }
        assert!(self.cap() - self.write as usize >= data.len());
        // copy data to buffer
        unsafe {
            copy(
                data.as_ptr(),
                self.data.as_ptr().offset(self.write as isize),
                data.len(),
            );
        }
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
    #[inline]
    fn resize(&mut self, new: usize) {
        if new > 256 * 1024 * 1024 {
            println!(
                "resize self: {:p} tx_buf read:{} write:{} cap:{} MemPolicy:{:?} len:{} head: {:?}",
                &self,
                self.read,
                self.write,
                self.cap,
                self.policy,
                self.data().len(),
                &self.data()[0..self.cap.min(2048) as usize]
            );
        }

        assert!(new < u32::MAX as usize);
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
                        (self.write - self.read) as usize,
                    );
                }
            }
            data
        } else {
            NonNull::dangling()
        };
        // 释放老的data
        if self.cap > 0 {
            let _old = unsafe { Vec::<u8>::from_raw_parts(self.data.as_ptr(), 0, self.cap()) };
            ds::BUF_TX.decr_by(self.cap());
        }
        self.data = data;
        self.cap = new as u32;
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
