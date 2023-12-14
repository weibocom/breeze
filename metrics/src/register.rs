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
    register: Sender<Op>,
    len: usize,
    id_idx: HashMap<Arc<Id>, usize>,
    idx_id: Vec<Arc<Id>>,
    cache: Vec<(usize, Arc<Box<Item>>)>,
}

enum Op {
    Local((Arc<Id>, Box<Item>)),
}

impl Metrics {
    fn new(register: Sender<Op>) -> Self {
        let mut me = Self {
            // 在metric register handler中，按需要扩容chunks
            chunks: Vec::new(),
            register,
            len: 0,
            id_idx: Default::default(),
            idx_id: Default::default(),
            cache: Default::default(),
        };
        me.reserve_chunk_num(1);
        me
    }
    pub(crate) fn register(&self, id: Id) -> Metric {
        if let Some(&idx) = self.id_idx.get(&id) {
            let item = self.get_item(idx);
            return Metric::from(item);
        }
        let id = Arc::new(id);
        // 从local中获取
        let item = Box::new(Item::local(id.clone()));
        let metric = Metric::from(&*item);
        let _r = self.register.send(Op::Local((id, item)));
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
        log::info!(
            "get item:{} => {} {}, {} ",
            idx,
            slot,
            offset,
            self.chunks.len()
        );
        unsafe { &*self.chunks.get_unchecked(slot).offset(offset as isize) }
    }
    #[inline]
    fn reserve(&mut self, id: Arc<Id>, item: Box<Item>) {
        log::info!("reserve metric:{} => {:?}", self.len, id);
        self.reserve_chunk_num(1);
        assert!(item.is_local());
        let idx = *self.id_idx.entry(id.clone()).or_insert(self.len);
        if idx == self.len() {
            log::info!("new metric registered:{} => {:?}", idx, id);
            self.len += 1;
            for _i in self.idx_id.len()..=idx {
                self.idx_id.push(Default::default());
            }
            assert!(idx < self.idx_id.len());
            assert!(self.idx_id[idx].empty());
            self.idx_id[idx] = id.clone();
            log::info!("new metric registered complete:{} => {:?}", idx, id);
        }
        // index不存在
        //assert_eq!(*self.id_idx.get(&id).expect("none"), idx);

        // 处理local的item
        self.cache.push((idx, item.into()));
    }
    // 把cache合并到global
    #[inline]
    pub(crate) fn flush_cache(&mut self) {
        for (idx, local) in &self.cache {
            let idx = *idx;
            assert!(idx < self.len());
            let (id, item) = self.get_item_id(idx);
            use crate::Snapshot;
            id.t.merge(item.data0(), local.data0());
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
pub(crate) fn get_item(id: &Id) -> Option<*const Item> {
    let metrics = get_metrics();
    metrics
        .id_idx
        .get(id)
        .map(|&idx| metrics.get_item(idx) as *const _)
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
    rx: Receiver<Op>,
    metrics: CowWriteHandle<Metrics>,
}

impl MetricRegister {
    fn new(rx: Receiver<Op>, metrics: CowWriteHandle<Metrics>) -> Self {
        Self { rx, metrics }
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

use std::task::ready;

impl Future for MetricRegister {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = &mut *self;
        //let mut t = me.metrics.copy();
        let mut t = None;
        log::info!("metric register poll");
        loop {
            let ret = me.rx.poll_recv(cx);
            if let Poll::Ready(Some(op)) = ret {
                let (id, local) = match op {
                    Op::Local((id, item)) => (id, item),
                };
                let t = t.get_or_insert_with(|| me.metrics.copy());
                t.reserve(id, local);
                continue;
            }

            let mut cache = false;
            if let Some(mut t) = t {
                t.flush_cache();
                cache = t.cache.len() > 0;
                me.metrics.update(t);
            }

            if cache {
                // sleep 1秒，等待cache合并到global
            }

            let _r = ready!(ret);
            panic!("metric register channel closed");
        }
    }
}
