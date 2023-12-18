use ds::time::{interval, Duration, Instant};
use std::sync::Arc;

use crate::{Id, Item, Metric};

const CHUNK_SIZE: usize = 4096;

use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedReceiver as Receiver, UnboundedSender as Sender},
    time::Interval,
};

#[derive(Clone)]
pub struct Metrics {
    // chunks只扩容，不做其他变更。
    chunks: Vec<*const Item>,
    register: Sender<Op>,
    len: usize,
    id_idx: HashMap<Arc<Id>, usize>,
    idx_id: Vec<Arc<Id>>,
}

//pub type ItemPtr = Box<Item>;
pub struct ItemPtr {
    ptr: usize,
}
impl ItemPtr {
    fn local(id: Arc<Id>) -> Self {
        let item = Box::leak(Box::new(Item::local(id)));
        Self {
            ptr: item as *const _ as usize,
        }
    }
}
// 实现Deref
impl std::ops::Deref for ItemPtr {
    type Target = Item;

    fn deref(&self) -> &Self::Target {
        unsafe { &*(self.ptr as *const Item) }
    }
}
impl Drop for ItemPtr {
    fn drop(&mut self) {
        let raw = self.ptr as *mut Item;
        let _ = unsafe { Box::from_raw(raw) };
    }
}

pub enum Op {
    Local((Arc<Id>, ItemPtr)),
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
        let item = ItemPtr::local(id.clone());
        let metric = Metric::from(&*item as _);
        log::debug!("register sent {id:?}");
        let _r = self.register.send(Op::Local((id, item)));
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
    fn reserve_idx(&mut self, id: &Arc<Id>) -> usize {
        self.reserve_chunk_num(1);
        let idx = *self.id_idx.entry(id.clone()).or_insert(self.len);
        if idx == self.len() {
            self.len += 1;
            for _i in self.idx_id.len()..=idx {
                self.idx_id.push(Default::default());
            }
            assert!(idx < self.idx_id.len());
            assert!(self.idx_id[idx].empty());
            self.idx_id[idx] = id.clone();
        }
        idx
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
pub mod tests {
    use super::*;
    pub fn init_metrics_onlyfor_test() -> Receiver<Op> {
        let (register_tx, chan_rx) = unbounded_channel();
        let (_tx, rx) = ds::cow(Metrics::new(register_tx));
        let _ = METRICS.set(rx).map_err(|_e| panic!("init"));
        chan_rx
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
    tick: Interval,
    ops: Vec<(usize, ItemPtr)>,
    cache: Option<Metrics>,
    last_ops: Instant,
}

impl MetricRegister {
    fn new(rx: Receiver<Op>, metrics: CowWriteHandle<Metrics>) -> Self {
        Self {
            rx,
            metrics,
            tick: interval(Duration::from_secs(1)),
            ops: Vec::new(),
            cache: None,
            last_ops: Instant::now(),
        }
    }
    fn get_metrics(&mut self) -> &mut Metrics {
        self.cache.get_or_insert_with(|| self.metrics.copy())
    }
    fn get_metric_ops(&mut self) -> (&mut Metrics, &mut Vec<(usize, ItemPtr)>) {
        let Self { ops, .. } = self;
        let metrics = self.cache.get_or_insert_with(|| self.metrics.copy());
        (metrics, ops)
    }
    // op被清理的条件：
    // metric主动断开与item的连接(detach之后，一般是metric已经获取到了global的item，或者已经删除)
    // 1. item deattach是metric主动调用，说明item可以安全释放；
    fn process_ops(&mut self) {
        if self.ops.len() == 0 || self.last_ops.elapsed() <= Duration::from_secs(32) {
            return;
        }
        self.last_ops = Instant::now();
        use crate::Snapshot;
        let (metrics, ops) = self.get_metric_ops();
        ops.retain(|(idx, local)| {
            let (gid, global) = metrics.get_item_id(*idx);
            let id: &Id = &*local.id();
            assert_eq!(gid, id, "id not equal");

            id.t.merge(global.data0(), local.data0());
            local.is_attached()
        });
        let new_cap = (ops.capacity() / 2).max(32);
        ops.shrink_to(new_cap);
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
            if let Poll::Ready(Some(Op::Local((id, item)))) = ret {
                assert!(item.is_local());
                let idx = me.get_metrics().reserve_idx(&id);
                me.ops.push((idx, item));
                continue;
            }
            if me.cache.is_none() && me.ops.len() == 0 {
                let _r = ready!(ret);
                panic!("register channel closed");
            }

            // 控制更新频繁
            ready!(me.tick.poll_tick(cx));
            me.process_ops();
            if let Some(t) = me.cache.take() {
                log::info!("metrics updated:{}", me.ops.len());
                me.metrics.update(t);
            }
        }
    }
}
