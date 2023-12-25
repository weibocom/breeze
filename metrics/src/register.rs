use ds::time::{interval, Duration};
use std::collections::HashMap;
use std::sync::Arc;

use crate::{Id, Item, ItemData, Metric};

const CHUNK_SIZE: usize = 4096;

use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedReceiver as Receiver, UnboundedSender as Sender},
    time::Interval,
};

#[derive(Clone)]
pub struct Metrics {
    // chunks只扩容，不做其他变更。
    chunks: Vec<*const Item>,
    len: usize,
    id_idx: HashMap<Arc<Id>, usize>,
    idx_id: Vec<Arc<Id>>,
}

enum Op {
    Register(Arc<Id>),
    Flush(Arc<Id>, ItemData),
}
use crate::ItemPtr;

impl Metrics {
    fn new() -> Self {
        let mut me = Self {
            // 在metric register handler中，按需要扩容chunks
            chunks: Vec::new(),
            len: 0,
            id_idx: Default::default(),
            idx_id: Default::default(),
        };
        me.reserve_chunk_num(1);
        me
    }
    fn register(&self, id: Id) -> Metric {
        if let Some(&idx) = self.id_idx.get(&id) {
            let item = self.get_item(idx);
            let item = ItemPtr::global(item);
            return Metric::from(item);
        }
        let id = Arc::new(id);
        // 从local中获取
        let item = ItemPtr::local(id.clone());
        let metric = Metric::from(item);
        log::debug!("register sent {id:?}");
        let _r = get_register().send(Op::Register(id));
        assert!(_r.is_ok());
        return metric;
    }
    fn cap(&self) -> usize {
        self.chunks.len() * CHUNK_SIZE
    }
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.len
    }
    #[inline]
    pub(crate) fn get_item_id(&self, idx: usize) -> (&Id, &Item) {
        let item = self.get_item(idx);
        assert!(idx < self.idx_id.len());
        let id = self.idx_id.get(idx).expect("id");
        (id, item)
    }
    pub(crate) fn get_item(&self, idx: usize) -> &Item {
        assert!(idx < self.len);
        assert!(idx < self.cap());
        let slot = idx / CHUNK_SIZE;
        let offset = idx % CHUNK_SIZE;
        assert!(slot < self.chunks.len());
        unsafe { &*self.chunks.get_unchecked(slot).offset(offset as isize) }
    }
    #[inline]
    fn reserve_idx(&mut self, id: &Arc<Id>) {
        self.reserve_chunk_num(1);
        let idx = *self.id_idx.entry(id.clone()).or_insert(self.len);
        if idx == self.len() {
            self.len += 1;
            self.idx_id.push(id.clone());
            assert_eq!(self.len, self.idx_id.len());
        }
    }
    #[inline]
    fn reserve_chunk_num(&mut self, n: usize) {
        if self.len + n < self.cap() {
            return;
        }
        let num = ((self.len + n + CHUNK_SIZE) - self.cap()) / CHUNK_SIZE;
        let mut oft = self.chunks.len() * CHUNK_SIZE;
        for _i in 0..num {
            let chunk: Vec<Item> = (0..CHUNK_SIZE).map(|j| Item::global(oft + j)).collect();
            let leaked = Box::leak(Box::new(chunk));
            self.chunks.push(leaked.as_mut_ptr());
            oft += CHUNK_SIZE;
        }
        log::info!("chunks scaled:{}", self);
    }
}

#[inline]
pub(crate) fn get_metrics() -> ReadGuard<Metrics> {
    METRICS.get().unwrap().get()
}

#[inline]
pub(crate) fn register_metric(id: Id) -> Metric {
    get_metrics().register(id)
}
#[inline]
pub(crate) fn flush_item(item: &Item) {
    debug_assert!(item.is_local());
    let id = item.id();
    use crate::Snapshot;
    if id.t.is_empty(item.data()) {
        return;
    }
    if let Some(global) = get_item(&*id) {
        debug_assert!(!global.is_null());
        let global = unsafe { &*global };
        id.t.merge(global.data(), item.data());
    } else {
        // 如果global不存在，则将当前的item异步flush到global
        let data = ItemData::default();
        id.t.merge(&data, item.data());
        let _r = get_register().send(Op::Flush(id.clone(), data));
        assert!(_r.is_ok());
    }
}
pub(crate) fn with_metric_id<O>(idx: usize, mut f: impl FnMut(&Id) -> O) -> O {
    f(get_metrics().get_item_id(idx).0)
}
#[inline]
pub(crate) fn get_item(id: &Id) -> Option<*const Item> {
    let metrics = get_metrics();
    metrics
        .id_idx
        .get(id)
        .map(|&idx| metrics.get_item(idx) as *const _)
}

use once_cell::sync::OnceCell;
static METRICS: OnceCell<CowReadHandle<Metrics>> = OnceCell::new();
static mut SENDER: Option<Sender<Op>> = None;
fn get_register() -> &'static Sender<Op> {
    unsafe { SENDER.as_ref().expect("not inited") }
}
pub mod tests {
    use super::*;
    static mut TEST_RECEIVER: Option<Receiver<Op>> = None;
    pub fn init_metrics_onlyfor_test() {
        let (register_tx, chan_rx) = unbounded_channel();
        let (_tx, rx) = ds::cow(Metrics::new());
        unsafe { SENDER = Some(register_tx) };
        let _ = METRICS.set(rx).map_err(|_e| panic!("init"));
        unsafe { TEST_RECEIVER = Some(chan_rx) };
    }
}

use ds::{CowReadHandle, CowWriteHandle, ReadGuard};

unsafe impl Sync for Metrics {}
unsafe impl Send for Metrics {}
use std::fmt::{self, Display, Formatter};
impl Display for Metrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "len:{} cap:{}", self.len, self.cap(),)
    }
}

pub struct MetricRegister {
    rx: Receiver<Op>,
    metrics: CowWriteHandle<Metrics>,
    tick: Interval,
    cache: Option<Metrics>,
}

impl MetricRegister {
    fn new(rx: Receiver<Op>, metrics: CowWriteHandle<Metrics>) -> Self {
        Self {
            rx,
            metrics,
            tick: interval(Duration::from_secs(1)),
            cache: None,
        }
    }
    fn get_metrics(&mut self) -> &mut Metrics {
        self.cache.get_or_insert_with(|| self.metrics.copy())
    }
}
impl Default for MetricRegister {
    fn default() -> Self {
        log::info!("task started ==> metric register");
        assert!(METRICS.get().is_none());
        assert!(unsafe { SENDER.is_none() });
        let (register_tx, register_rx) = unbounded_channel();
        let (tx, rx) = ds::cow(Metrics::new());
        let _ = METRICS.set(rx);
        // 设置全局的SENDER
        unsafe { SENDER = Some(register_tx) };
        MetricRegister::new(register_rx, tx)
    }
}

use std::{
    future::Future,
    pin::Pin,
    task::{ready, Context, Poll},
};

impl Future for MetricRegister {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = &mut *self;
        log::debug!("metric register poll");
        loop {
            let ret = me.rx.poll_recv(cx);
            if let Poll::Ready(Some(op)) = ret {
                match op {
                    Op::Register(id) => {
                        me.get_metrics().reserve_idx(&id);
                    }
                    Op::Flush(id, local) => {
                        let metrics = me.get_metrics();
                        // 一定是已经注册的
                        let idx = *metrics.id_idx.get(&id).expect("id not registered");
                        let global = metrics.get_item(idx);
                        use crate::Snapshot;
                        id.t.merge(global.data(), &local);
                    }
                }
                continue;
            }
            if me.cache.is_none() {
                let _r = ready!(ret);
                panic!("register channel closed");
            }

            // 控制更新频繁
            ready!(me.tick.poll_tick(cx));
            let t = me.cache.take().expect("cache");
            log::debug!("metrics updated:{}", t);
            me.metrics.update(t);
        }
    }
}
