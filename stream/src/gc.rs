use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use ds::AtomicWaker;
use enum_dispatch::enum_dispatch;
use futures::ready;

use crate::buffer::StreamGuard;
use crate::CallbackContextPtr;

#[enum_dispatch]
pub(crate) trait Until {
    fn droppable(&mut self) -> bool;
}

pub struct DelayedDrop<T> {
    inner: *mut T,
}
use std::ops::{Deref, DerefMut};
impl<T> Deref for DelayedDrop<T> {
    type Target = T;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        unsafe { &*self.inner }
    }
}
impl<T> DerefMut for DelayedDrop<T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.inner }
    }
}
impl<T> From<T> for DelayedDrop<T> {
    #[inline(always)]
    fn from(t: T) -> Self {
        let b = Box::new(t);
        Self {
            inner: Box::leak(b),
        }
    }
}
impl<T> Drop for DelayedDrop<T> {
    #[inline(always)]
    fn drop(&mut self) {
        debug_assert!(!self.inner.is_null());
        unsafe {
            let _dropped = Box::from_raw(self.inner);
        }
    }
}

type Pipeline = (
    DelayedDrop<StreamGuard>,
    DelayedDrop<VecDeque<CallbackContextPtr>>,
    DelayedDrop<AtomicWaker>,
);
#[enum_dispatch(Until)]
pub enum Delayed {
    Handler(DelayedDrop<StreamGuard>), // 从handler释放的
    Pipeline(Pipeline),                // 从pipeline请求过来的
}

// 某些struct需要在满足某些条件之后才能删除。
pub(crate) fn delayed_drop<T: Until + Into<Delayed>>(mut t: T) {
    if !t.droppable() {
        let d = t.into();
        log::debug!("an instance delay dropped");
        debug_assert!(SENDER.get().is_some());
        unsafe {
            let _ = SENDER.get_unchecked().send(d.into());
        }
    }
}
impl Until for StreamGuard {
    #[inline]
    fn droppable(&mut self) -> bool {
        self.gc();
        log::debug!("handler buf pending:{}", self.pending());
        self.pending() == 0
    }
}
impl Until for Pipeline {
    #[inline]
    fn droppable(&mut self) -> bool {
        let queue = &mut self.1;
        while let Some(ctx) = queue.front() {
            if ctx.complete() {
                queue.pop_front();
            } else {
                break;
            }
        }
        let buf = &mut self.0;
        buf.gc();
        log::debug!("pipeline buff:{} queue:{}", buf.pending(), queue.len());
        buf.pending() == 0 && queue.len() == 0
    }
}
impl<T: Until> Until for DelayedDrop<T> {
    #[inline]
    fn droppable(&mut self) -> bool {
        self.deref_mut().droppable()
    }
}

use once_cell::sync::OnceCell;
static SENDER: OnceCell<Sender<DelayedByTime<Delayed>>> = OnceCell::new();
use tokio::sync::mpsc::{
    unbounded_channel, UnboundedReceiver as Receiver, UnboundedSender as Sender,
};
pub fn start_delay_drop() -> DelayedDropHandler {
    let (tx, rx) = unbounded_channel();
    SENDER.set(tx).expect("inited yet");

    log::info!("task started ==> delayed instance gc");

    DelayedDropHandler {
        rx,
        tick: interval(Duration::from_secs(1)),
        cache: None,
    }
}
use tokio::time::{interval, Duration, Instant, Interval};
pub struct DelayedDropHandler {
    rx: Receiver<DelayedByTime<Delayed>>,
    tick: Interval,
    cache: Option<DelayedByTime<Delayed>>,
}
impl Future for DelayedDropHandler {
    type Output = ();

    #[inline(always)]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            ready!(self.tick.poll_tick(cx));
            if let Some(ref mut d) = self.cache {
                if d.droppable() {
                    let _ = self.cache.take();
                } else {
                    // 一次只删除第一个
                    continue;
                }
            }
            debug_assert!(self.cache.is_none());
            while let Poll::Ready(Some(mut d)) = self.rx.poll_recv(cx) {
                if d.droppable() {
                    drop(d);
                    continue;
                }
                // 不释放。已经poll的先临时cache下来
                self.cache = Some(d);
                break;
            }
        }
    }
}

struct DelayedByTime<T> {
    inner: T,
    start: Instant,
}

impl<T> From<T> for DelayedByTime<T> {
    #[inline]
    fn from(t: T) -> Self {
        Self {
            inner: t,
            start: Instant::now(),
        }
    }
}
impl<T: Until> Until for DelayedByTime<T> {
    #[inline]
    fn droppable(&mut self) -> bool {
        self.inner.droppable() || self.start.elapsed() >= Duration::from_secs(15)
    }
}

unsafe impl<T: Send> Send for DelayedDrop<T> {}
unsafe impl<T: Sync> Sync for DelayedDrop<T> {}
