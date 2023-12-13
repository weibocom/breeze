use std::sync::Arc;

use crate::{Id, IdSequence, Item, Metric};

const CHUNK_SIZE: usize = 4096;

use tokio::sync::mpsc::{
    unbounded_channel, UnboundedReceiver as Receiver, UnboundedSender as Sender,
};

#[derive(Clone)]
pub struct Metrics {
    // chunks只扩容，不做其他变更。
    chunks: Vec<*const Item>,
    register: Sender<(Arc<Id>, i64)>,
    len: usize,
    id_idx: IdSequence,
}

impl Metrics {
    fn new(register: Sender<(Arc<Id>, i64)>) -> Self {
        Self {
            // 在metric register handler中，按需要扩容chunks
            chunks: Vec::new(),
            register,
            len: 0,
            id_idx: Default::default(),
        }
    }
    pub(crate) fn register(&self, id: Id) -> Metric {
        let id = Arc::new(id);
        self.try_send_register(id.clone());
        Metric::from(id)
    }
    // 在初始化完成之前，部分数据需要先缓存处理。
    // 只缓存Count类型的数据
    #[inline]
    pub(crate) fn cache(&self, id: &Arc<Id>, cache: i64) {
        if id.t.need_flush() {
            if let Err(_e) = self.register.send((id.clone(), cache)) {
                log::info!("cache error. id:{:?} cache:{} {:?}", id, cache, _e);
            }
        }
    }
    pub(crate) fn try_send_register(&self, id: Arc<Id>) {
        if self.id_idx.get_idx(&id).is_none() {
            // 需要注册。可能会多次重复注册，在接收的时候去重处理。
            log::debug!("metric registering {:?}", id);
            if let Err(_e) = self.register.send((id.clone(), 0)) {
                log::info!("send register metric failed. {:?} id:{:?}", _e, id)
            };
        }
    }
    fn init(&mut self, id: Arc<Id>) {
        // 检查是否已经初始化
        if let Some(idx) = self.id_idx.get_idx(&id) {
            assert!(self.get_item(idx).inited());
            return;
        }
        let idx = self.id_idx.register_name(&id);
        self.reserve(idx);
        self.len = (idx + 1).max(self.len);
        if let Some(mut item) = self.get_item(idx).try_lock() {
            log::debug!("item inited:{:?}", id);
            item.init(id);
            return;
        }
        log::warn!("failed to aquire metric lock. idx:{}", idx);
    }
    fn cap(&self) -> usize {
        self.chunks.len() * CHUNK_SIZE
    }
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.len
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
    fn check_and_get_item(&self, id: &Arc<Id>) -> Option<*const Item> {
        if let Some(idx) = self.id_idx.get_idx(id) {
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
    //pub(crate) fn write<W: crate::ItemWriter>(&self, w: &mut W, secs: f64) {
    //    for i in 0..self.len {
    //        let item = self.get_item(i);
    //        if item.inited() {
    //            item.snapshot(w, secs);
    //        }
    //    }
    //}
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
pub(crate) fn register_cache(id: &Arc<Id>, cache: i64) {
    get_metrics().cache(id, cache)
}
#[inline]
pub(crate) fn get_metric(id: &Arc<Id>) -> Option<*const Item> {
    get_metrics().check_and_get_item(id)
}

use once_cell::sync::OnceCell;
static METRICS: OnceCell<CowReadHandle<Metrics>> = OnceCell::new();
pub mod tests {
    use super::*;
    pub fn init_metrics_onlyfor_test() {
        let (register_tx, _) = unbounded_channel();
        let (_, rx) = ds::cow(Metrics::new(register_tx));
        let _ = METRICS.set(rx);
    }
}

use ds::{CowReadHandle, CowWriteHandle, ReadGuard};

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

use std::collections::HashMap;
pub struct MetricRegister {
    rx: Receiver<(Arc<Id>, i64)>,
    metrics: CowWriteHandle<Metrics>,
    tick: Interval,
    cache: Option<HashMap<Arc<Id>, i64>>,
    metrics_r: Option<Metrics>,
}

impl MetricRegister {
    fn new(rx: Receiver<(Arc<Id>, i64)>, metrics: CowWriteHandle<Metrics>) -> Self {
        let mut tick = interval(Duration::from_secs(3));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        Self {
            rx,
            metrics,
            tick,
            cache: None,
            metrics_r: None,
        }
    }
    fn process_one(&mut self, id: Arc<Id>, cache: i64) {
        if cache == 0 {
            // 说明是初始化
            self.metrics_r
                .get_or_insert_with(|| self.metrics.copy())
                .init(id);
        } else {
            *self
                .cache
                .get_or_insert_with(|| HashMap::with_capacity(128))
                .entry(id)
                .or_insert(0) += cache;
        }
    }
}
impl Default for MetricRegister {
    fn default() -> Self {
        log::info!("task started ==> metric register");
        assert!(METRICS.get().is_none());
        let (register_tx, register_rx) = unbounded_channel();
        let (tx, rx) = ds::cow(Metrics::new(register_tx));
        let _ = METRICS.set(rx);
        MetricRegister::new(register_rx, tx)
    }
}

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use ds::time::{interval, Duration, Interval};
use std::task::ready;

impl Future for MetricRegister {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = &mut *self;
        loop {
            while let Poll::Ready(Some((id, cache))) = me.rx.poll_recv(cx) {
                me.process_one(id, cache);
            }
            // 注册。
            if let Some(metrics) = me.metrics_r.take() {
                me.metrics.update(metrics);
            }
            // flush cache
            if let Some(mut cache) = me.cache.take() {
                let metrics = me.metrics.get();
                for (id, v) in cache.iter_mut() {
                    if let Some(item) = metrics.check_and_get_item(id) {
                        // 已经初始化完成，flush cache
                        unsafe { (&*item).data().flush(*v) };
                        *v = 0;
                    }
                }
                // 删除所有cache为0的值
                cache.retain(|_k, v| *v > 0);
                if cache.len() > 0 {
                    me.cache = Some(cache);
                }
                // 有更新，说明channel里面可能还有数据等待处理。
                continue;
            }
            ready!(me.tick.poll_tick(cx));
        }
    }
}
