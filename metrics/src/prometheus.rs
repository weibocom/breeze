use std::sync::{
    atomic::{
        AtomicUsize,
        Ordering::{AcqRel, Acquire},
    },
    Arc,
};
pub struct Prometheus {
    secs: f64,
    idx: Arc<AtomicUsize>,
}

impl Prometheus {
    pub fn new(secs: f64) -> Self {
        let idx = Arc::new(AtomicUsize::new(0));
        Self { idx, secs }
    }
}

use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

impl futures::Stream for Prometheus {
    type Item = PrometheusItem;
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let idx = self.idx.load(Acquire);
        let len = crate::get_metrics().len();
        if idx < len {
            Poll::Ready(Some(PrometheusItem::new(&self.idx, self.secs)))
        } else {
            Poll::Ready(None)
        }
    }
}

pub struct PrometheusItem {
    idx: Arc<AtomicUsize>,
    left: Vec<u8>,
    secs: f64,
}
impl PrometheusItem {
    pub fn new(idx: &Arc<AtomicUsize>, secs: f64) -> Self {
        Self {
            left: Vec::new(),
            idx: idx.clone(),
            secs,
        }
    }
}

impl AsyncRead for PrometheusItem {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let metrics = crate::get_metrics();
        let len = metrics.len();
        if self.left.len() > 0 {
            let n = std::cmp::min(self.left.len(), buf.remaining());
            let left = self.left.split_off(n);
            buf.put_slice(&self.left);
            self.left = left;
        }
        while buf.remaining() > 0 {
            let idx = self.idx.fetch_add(1, AcqRel);
            if idx >= len {
                break;
            }
            let mut w = PrometheusItemWriter::new(buf);
            if idx == 0 {
                let mut host = HOST.try_lock().expect("host lock");
                host.snapshot(&mut w, self.secs);
            }
            let item = metrics.get_item(idx);
            if item.inited() {
                item.snapshot(&mut w, self.secs);
                self.left = w.left();
            }
        }
        Poll::Ready(Ok(()))
    }
}

struct PrometheusItemWriter<'a, 'r> {
    left: Vec<u8>,
    buf: &'a mut ReadBuf<'r>,
}
impl<'a, 'r> PrometheusItemWriter<'a, 'r> {
    fn new(buf: &'a mut ReadBuf<'r>) -> Self {
        Self {
            left: Vec::new(),
            buf,
        }
    }
    #[inline]
    fn left(self) -> Vec<u8> {
        self.left
    }
    #[inline]
    fn put_slice(&mut self, data: &[u8]) {
        if self.buf.remaining() >= data.len() {
            self.buf.put_slice(data);
        } else {
            let n = std::cmp::min(data.len(), self.buf.remaining());
            let (f, s) = data.split_at(n);
            self.buf.put_slice(&f);
            self.left.extend_from_slice(&s);
        }
    }
}
impl<'a, 'r> crate::ItemWriter for PrometheusItemWriter<'a, 'r> {
    fn write(&mut self, name: &str, key: &str, sub_key: &str, val: f64) {
        self.put_slice(name.as_bytes());
        self.put_slice(key.as_bytes());
        self.put_slice(sub_key.as_bytes());
        self.put_slice(val.to_string().as_bytes());
        self.put_slice(b"\r\n");
    }
}
use crate::Host;
use ds::lock::Lock;
use once_cell::sync::Lazy;
static HOST: Lazy<Lock<Host>> = Lazy::new(|| Host::new().into());
pub fn init() {
    let _ = HOST.try_lock().expect("host lock");
}
