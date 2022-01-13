use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use discovery::{TopologyRead, TopologyReadGuard};
use ds::Switcher;
use protocol::{Endpoint, Topology};
use sharding::hash::Hasher;

// 支持刷新
pub struct RefreshTopology<T> {
    ep: Arc<AtomicPtr<T>>,
}
impl<T: Clone + 'static> RefreshTopology<T> {
    // reader一定是已经初始化过的，否则会UB
    #[inline]
    pub fn new(reader: TopologyReadGuard<T>, refresh: Switcher) -> Self {
        let ep = Default::default();
        let me = Self { ep };
        let mut r = Refresher::new(refresh, reader, me.ep.clone());
        r.refresh();
        r.start_refresh();
        me
    }
    #[inline]
    fn top(&self) -> &T {
        // Relaxed是安全的，因为异步更新会确保内存可见性与销毁策略。
        unsafe { &*self.ep.load(Ordering::Relaxed) }
    }
}
impl<T: Endpoint + Clone + 'static> Endpoint for RefreshTopology<T> {
    type Item = T::Item;
    #[inline(always)]
    fn send(&self, req: T::Item) {
        self.top().send(req);
    }
}
impl<T: Topology + Clone + 'static> Topology for RefreshTopology<T> {
    #[inline(always)]
    fn hasher(&self) -> &Hasher {
        self.top().hasher()
    }
}
impl<T: Endpoint + Clone + 'static> RefreshTopology<T> {
    #[inline(always)]
    pub fn static_send<R: Into<T::Item>>(receiver: usize, req: R) {
        let req = req.into();
        let top = receiver as *const Self;
        unsafe { (&*top).send(req) };
    }
}

unsafe impl<T> Send for RefreshTopology<T> {}
unsafe impl<T> Sync for RefreshTopology<T> {}
use tokio::time::{interval, Interval};

struct Refresher<T> {
    switcher: Switcher,
    reader: TopologyReadGuard<T>,
    ep: Arc<AtomicPtr<T>>,
    dropping: Vec<usize>,
    last_time: Instant,
    last_cycle: usize,
    tick: Interval,
}
impl<T: Clone + 'static> Refresher<T> {
    fn new(switcher: Switcher, reader: TopologyReadGuard<T>, ep: Arc<AtomicPtr<T>>) -> Self {
        Self {
            switcher,
            reader,
            ep,
            dropping: Vec::new(),
            last_time: Instant::now(),
            last_cycle: 0,
            tick: interval(Duration::from_secs(7)),
        }
    }
    fn start_refresh(self) {
        rt::spawn(self);
    }
    fn refresh(&mut self) {
        self.last_cycle = self.reader.cycle();
        let ep = Box::leak(Box::new(self.reader.do_with(|t| t.clone())));
        let old = self.ep.swap(ep, Ordering::Relaxed);
        if !old.is_null() {
            self.dropping.push(old as usize);
        }
        self.last_time = Instant::now();
    }
    // 连续超过30秒钟没有refresh，则销毁所有dropping实例。
    fn try_clear(&mut self) {
        if self.dropping.len() > 0 && self.last_time.elapsed() >= Duration::from_secs(30) {
            for ep in self.dropping.split_off(0) {
                unsafe { std::ptr::drop_in_place(ep as *mut T) };
            }
            debug_assert_eq!(self.dropping.len(), 0);
        }
    }
    fn check(&self) -> bool {
        self.reader.cycle() > self.last_cycle
    }
}
use futures::ready;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
impl<T: Clone + 'static> Future for Refresher<T> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = &mut *self;
        while me.switcher.get() {
            ready!(me.tick.poll_tick(cx));
            if me.check() {
                me.refresh();
            }
            me.try_clear();
        }
        Poll::Ready(())
    }
}
