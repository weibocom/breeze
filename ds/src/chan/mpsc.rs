use core::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering::*};
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::Switcher;

pub enum TrySendError<T> {
    Closed(T),
    Full(T),
    Disabled(T),
}

pub fn channel<T>(cap: usize) -> (Sender<T>, Receiver<T>) {
    let s: Switcher = false.into();
    let (tx, rx) = tokio::sync::mpsc::channel(cap);
    let num = Arc::new(AtomicUsize::new(0));
    let tx = Sender {
        switcher: s.clone(),
        inner: tx,
        tx: num.clone(),
    };
    let rx = Receiver {
        switcher: s,
        inner: rx,
        rx: 0,
        tx: num,
    };
    (tx, rx)
}

pub struct Receiver<T> {
    rx: usize,
    switcher: Switcher,
    inner: tokio::sync::mpsc::Receiver<T>,
    tx: Arc<AtomicUsize>,
}
pub struct Sender<T> {
    tx: Arc<AtomicUsize>,
    switcher: Switcher,
    inner: tokio::sync::mpsc::Sender<T>,
}

impl<T> Receiver<T> {
    #[inline(always)]
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.inner.poll_recv(cx).map(|t| {
            t.map(|v| {
                self.rx += 1;
                // TODO 收到请求后，tx需要减1?
                self.tx.fetch_sub(1, AcqRel);
                v
            })
        })
    }
    #[inline(always)]
    pub fn has_multi(&mut self) -> bool {
        self.tx.load(Acquire) >= self.rx + 2
    }
    pub fn enable(&mut self) {
        // assert_eq!(self.tx.load(Acquire), self.rx, "not empty after disable");
        self.rx = 0;
        self.tx.store(0, Release);
        self.switcher.on();
    }
    pub fn disable(&mut self) {
        self.switcher.off();
    }
    pub fn is_empty_hint(&mut self) -> bool {
        self.rx >= self.tx.load(Acquire)
    }
}
impl<T> Debug for Receiver<T> {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "mpsc Receiver rx/tx:({},{}) switcher:{} inner:{:?}",
            self.rx,
            self.tx.load(Acquire),
            self.switcher.get(),
            self.inner
        )
    }
}

impl<T> Sender<T> {
    #[inline]
    pub fn try_send(&self, message: T) -> Result<(), TrySendError<T>> {
        if self.switcher.get() {
            let tx = self.tx.fetch_add(1, AcqRel);
            // TODO 测试完毕清理
            if tx > 5000 && tx % 1000 == 0 {
                log::info!("+++ channel appending count:{}", tx);
            }
            self.inner.try_send(message).map_err(|e| {
                self.tx.fetch_sub(1, AcqRel);
                match e {
                    tokio::sync::mpsc::error::TrySendError::Full(t) => {
                        log::info!("+++ channel full sender appending count: {}", tx);
                        TrySendError::Full(t)
                    }
                    tokio::sync::mpsc::error::TrySendError::Closed(t) => TrySendError::Closed(t),
                }
            })?;
            Ok(())
        } else {
            Err(TrySendError::Disabled(message))
        }
    }
}
