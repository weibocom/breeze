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
    #[inline]
    fn put_label(&mut self, name: &str, val: &[u8], isfirst: bool) {
        if val.len() > 0 {
            //isfirst 保证第一个label前没有 ","
            if isfirst {
                self.put_slice(b",");
            }
            self.put_slice(name.as_bytes());
            self.put_slice(b"=\"");
            self.put_slice(val);
            self.put_slice(b"\"");
        }
    }
}
impl<'a, 'r> crate::ItemWriter for PrometheusItemWriter<'a, 'r> {
    #[inline]
    fn write(&mut self, name: &str, key: &str, sub_key: &str, val: f64) {
        self.write_opts(name, key, sub_key, val, Vec::new());
    }
    fn write_opts(
        &mut self,
        name: &str,
        key: &str,
        sub_key: &str,
        val: f64,
        opts: Vec<(&str, &str)>,
    ) {
        /*
        三种类型: name 为 msgque_backend 且 namespace 中包含 ‘#’ 时增加一个 topic lable，‘#’ 前为namespace的值，‘#’ 后为topic的值
              name                                              key         sub_key         result
        <1>   base                                              host        mem             host_mem{source="base",pool="default_pool"} 31375360
        <2>   mc_backend/status.content1/127.0.0.1:8080         timeout     qps             timeout_qps{source="mc_backend",namespace="status.content1",bip="127.0.0.1:8080",pool="default_pool"} 0
        <3>   mc.$namespace                                     $key        $sub_key        $key_$sub_key{source="mc",namespace="$namespace",pool="default_pool"} 0
        <4>   msgque_backend                                    timeout     qps             timeout_qps{source="msgque_backend",pool="default_pool",namespace="mcq_common_feed2",topic="common_feed",bip="10.75.11.200:13791"} 0
         */

        //从 name 中截取 source、namespace和topic、instance
        let all_name: Vec<&str> = name.split("/").collect();
        let source = all_name.get(0).unwrap_or(&"").as_bytes();
        let charname = *all_name.get(1).unwrap_or(&"");
        let nameandtopic: Vec<&str> = charname.split("#").collect();
        let namespace = nameandtopic.get(0).unwrap_or(&"").as_bytes();
        let topic = nameandtopic.get(1).unwrap_or(&"").as_bytes();

        let bip = all_name.get(2).unwrap_or(&"").as_bytes();

        let mut name = String::new();
        let metrics_name = if sub_key.len() > 0 {
            name.reserve(key.len() + sub_key.len() + 1);
            name += key;
            name += "_";
            name += sub_key;
            &name
        } else {
            key
        };

        //promethues # HELP
        self.put_slice(b"# HELP ");
        self.put_slice(metrics_name.as_bytes());
        self.put_slice(b"\n");

        //promethues # TYPE
        self.put_slice(b"# TYPE ");
        self.put_slice(metrics_name.as_bytes());
        self.put_slice(b" gauge\n");

        //promethues metrics
        self.put_slice(metrics_name.as_bytes());
        self.put_slice("{".as_bytes());
        //确保第一个put的label一定不为空且isfirst值为false，后续只需isfirst传true即可; 后续优化
        self.put_label("source", source, false);
        self.put_label("pool", context::get().service_pool.as_bytes(), true);
        self.put_label("namespace", namespace, true);
        self.put_label("topic", topic, true);
        self.put_label("bip", bip, true);

        for (k, v) in opts {
            self.put_label(k, v.as_bytes(), true);
        }

        self.put_slice(b"}");

        //value
        self.put_slice(b" ");
        self.put_slice(val.to_string().as_bytes());
        self.put_slice(b"\n");
    }
}
use crate::Host;
use ds::lock::Lock;
use lazy_static::lazy_static;
lazy_static! {
    static ref HOST: Lock<Host> = Host::new().into();
}
