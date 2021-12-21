use std::sync::Arc;

use crate::{Id, Item, Metric};

const CHUNK_SIZE: usize = 4096;

use tokio::sync::mpsc::{
    unbounded_channel, UnboundedReceiver as Receiver, UnboundedSender as Sender,
};

#[derive(Clone)]
pub struct Metrics {
    // chunks只扩容，不做其他变更。
    chunks: Vec<*const Item>,
    register: Sender<(Arc<Id>, usize)>,
    len: usize,
}

impl Metrics {
    fn new(register: Sender<(Arc<Id>, usize)>) -> Self {
        Self {
            // 在metric register handler中，按需要扩容chunks
            chunks: Vec::new(),
            register,
            len: 0,
        }
    }
    pub(crate) fn register(&self, id: Id) -> Metric {
        let id = Arc::new(id);
        let (idx, inited) = crate::register_name(&id);
        if !inited {
            log::debug!("register request sent:{:?} idx:{}", id, idx);
            if let Err(e) = self.register.send((id, idx)) {
                log::info!("send register metric failed. id:{:?} idx:{}", e, idx)
            };
        }
        Metric::from(idx)
    }
    fn init(&mut self, id: Arc<Id>, idx: usize) {
        self.reserve(idx);
        self.len = (idx + 1).max(self.len);
        if let Some(mut item) = self.get_item(idx).try_lock() {
            item.init(id);
            return;
        }
        log::warn!("failed to aquire metric lock. idx:{}", idx);
    }
    fn cap(&self) -> usize {
        self.chunks.len() * CHUNK_SIZE
    }
    fn get_item(&self, idx: usize) -> &Item {
        debug_assert!(idx < self.len);
        debug_assert!(idx < self.cap());
        let slot = idx / CHUNK_SIZE;
        let offset = idx % CHUNK_SIZE;
        unsafe { &*self.chunks.get_unchecked(slot).offset(offset as isize) }
    }
    #[inline]
    fn check_and_get_item(&self, idx: usize) -> Option<*const Item> {
        if idx < self.len {
            let item = self.get_item(idx);
            if item.inited() {
                return Some(item);
            }
        }
        None
    }
    #[inline]
    fn reserve(&mut self, idx: usize) {
        if idx < self.cap() {
            return;
        }
        let num = ((idx + CHUNK_SIZE) - self.cap()) / CHUNK_SIZE;
        for _i in 0..num {
            let chunk: Vec<Item> = (0..CHUNK_SIZE).map(|_| Default::default()).collect();
            let leaked = Box::leak(Box::new(chunk));
            self.chunks.push(leaked.as_mut_ptr());
        }
        log::info!("chunks scaled:{}", self);
    }
    pub(crate) fn write<W: crate::ItemWriter>(&self, w: &mut W, secs: f64) {
        for i in 0..self.len {
            let item = self.get_item(i);
            if item.inited() {
                item.snapshot(w, secs);
            }
        }
    }
}

#[inline]
pub(crate) fn get_metrics<'a>() -> ReadGuard<'a, Metrics> {
    debug_assert!(METRICS.get().is_some());
    unsafe { METRICS.get_unchecked().get() }
}

#[inline]
pub(crate) fn register_metric(id: Id) -> Metric {
    debug_assert!(METRICS.get().is_some());
    get_metrics().register(id)
}
#[inline]
pub(crate) fn get_metric(idx: usize) -> Option<*const Item> {
    get_metrics().check_and_get_item(idx)
}

use ds::ReadGuard;
use once_cell::sync::OnceCell;
static METRICS: OnceCell<CowReadHandle<Metrics>> = OnceCell::new();

use ds::{CowReadHandle, CowWriteHandle};
pub fn start_register_metrics() {
    debug_assert!(METRICS.get().is_none());
    let (register_tx, register_rx) = unbounded_channel();
    let (tx, rx) = ds::cow(Metrics::new(register_tx));
    let _ = METRICS.set(rx);
    let registra = MetricRegister::new(register_rx, tx);
    tokio::spawn(registra);
}

unsafe impl Sync for Metrics {}
unsafe impl Send for Metrics {}
use std::fmt::{self, Display, Formatter};
impl Display for Metrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "len:{} cap:{} chunks:{}",
            self.len,
            self.cap(),
            self.chunks.len()
        )
    }
}

struct MetricRegister {
    rx: Receiver<(Arc<Id>, usize)>,
    metrics: CowWriteHandle<Metrics>,
    tick: Interval,
}

impl MetricRegister {
    fn new(rx: Receiver<(Arc<Id>, usize)>, metrics: CowWriteHandle<Metrics>) -> Self {
        let mut tick = interval(std::time::Duration::from_secs(3));
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        Self { rx, metrics, tick }
    }
}

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::ready;
use tokio::time::{interval, Interval, MissedTickBehavior};

impl Future for MetricRegister {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = &mut *self;
        loop {
            ready!(me.tick.poll_tick(cx));
            // 至少有一个，避免不必要的write请求
            if let Some((id, idx)) = ready!(me.rx.poll_recv(cx)) {
                me.metrics.write(|m| {
                    m.init(id.clone(), idx);
                    while let Poll::Ready(Some((id, idx))) = me.rx.poll_recv(cx) {
                        m.init(id, idx);
                    }
                });
            }
        }
    }
}
