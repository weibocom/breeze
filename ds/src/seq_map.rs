use std::task::{Context, Poll};

use futures::ready;
use tokio::sync::mpsc::{channel, error::TrySendError};

pub trait Sequential {
    // 数据是否读完，如果没读完，则会被重复read
    fn eof(&self) -> bool;
    // 是否需要编号。如果需要编号，则每次读完会先进行编号
    fn seq(&self) -> bool;
}

/// 数据无序写入，但读取获取是有序的。
/// 如果数据是只读的，则不需要保障顺序，并且数据一旦读取完成即可销毁。
pub fn bounded<T>(cap: usize) -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = channel::<T>(cap);
    (Sender { tx }, Receiver::new(rx))
}

use std::collections::VecDeque;
use std::mem::ManuallyDrop;
pub struct Receiver<T> {
    cache: *mut ManuallyDrop<T>,
    seqs: VecDeque<T>,
    rx: tokio::sync::mpsc::Receiver<T>,
}

impl<T> Receiver<T> {
    #[inline(always)]
    fn new(rx: tokio::sync::mpsc::Receiver<T>) -> Self {
        Self {
            cache: 0 as *mut ManuallyDrop<T>,
            rx,
            seqs: VecDeque::with_capacity(16 - 1),
        }
    }
    #[inline(always)]
    pub fn poll_read(&mut self, cx: &mut Context) -> Poll<&mut T>
    where
        T: Sequential,
    {
        if self.cache.is_null() {
            let req = ready!(self.rx.poll_recv(cx)).expect("channel closed");
            self.set(req);
        }
        // 数据已经读完
        debug_assert!(!self.cache.is_null());
        if self.cache().eof() {
            let req = self.take();
            if req.seq() {
                // 放入队列
                self.seqs.push_back(req);
            } // else: 直接销毁
            let req = ready!(self.rx.poll_recv(cx)).expect("channel closed");
            self.set(req);
        }
        Poll::Ready(self.cache())
    }
    #[inline(always)]
    fn cache(&mut self) -> &mut T {
        unsafe { std::mem::transmute(self.cache as usize as *mut T) }
    }
    #[inline(always)]
    fn set(&mut self, req: T) {
        debug_assert!(self.cache.is_null());
        let mut req = ManuallyDrop::new(req);
        self.cache = &mut req as *mut _;
    }
    #[inline(always)]
    fn take(&mut self) -> T {
        debug_assert!(!self.cache.is_null());
        let req: T = ManuallyDrop::into_inner(unsafe { self.cache.read() });
        self.cache = 0 as *mut _;
        req
    }
    // 取走上一次read过的数据。
    #[inline(always)]
    pub fn recv(&mut self) -> T {
        self.seqs
            .pop_front()
            .expect("poll_read not called. or no data exists")
    }
}

pub struct Sender<T> {
    tx: tokio::sync::mpsc::Sender<T>,
}

impl<T> Sender<T> {
    #[inline(always)]
    pub fn try_send(&self, t: T) -> Result<(), T> {
        if let Err(e) = self.tx.try_send(t) {
            match e {
                TrySendError::Closed(t) => Err(t),
                TrySendError::Full(t) => Err(t),
            }
        } else {
            Ok(())
        }
    }
}

unsafe impl<T> Send for Receiver<T> {}
unsafe impl<T> Sync for Receiver<T> {}
