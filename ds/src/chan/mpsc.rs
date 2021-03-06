use std::sync::atomic::{AtomicUsize, Ordering};
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
    #[inline]
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.inner.poll_recv(cx).map(|t| {
            self.rx += 1;
            t
        })
    }
    #[inline]
    pub fn size_hint(&mut self) -> usize {
        let tx = self.tx.load(Ordering::Relaxed);
        tx - tx.min(self.rx)
    }
    pub fn enable(&mut self) {
        self.switcher.on();
    }
    pub fn disable(&mut self) {
        self.switcher.off();
    }
}

impl<T> Sender<T> {
    #[inline]
    pub fn try_send(&self, message: T) -> Result<(), TrySendError<T>> {
        if self.switcher.get() {
            self.inner.try_send(message).map_err(|e| match e {
                tokio::sync::mpsc::error::TrySendError::Full(t) => TrySendError::Full(t),
                tokio::sync::mpsc::error::TrySendError::Closed(t) => TrySendError::Closed(t),
            })?;
            self.tx.fetch_add(1, Ordering::Relaxed);
            Ok(())
        } else {
            Err(TrySendError::Disabled(message))
        }
    }
}
