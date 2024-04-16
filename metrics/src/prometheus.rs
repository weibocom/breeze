use crate::{ItemWriter, WriteTo};
pub struct Prometheus {
    secs: f64,
    idx: usize,
    host: bool,       // host相关的metrics是否已经发送
    ext_buf: Vec<u8>, // 扩展的buffer，如果ReadBuf没有足够的空间，则使用这个buffer
    ext_oft: usize,
}

impl Prometheus {
    pub fn new(secs: f64) -> Self {
        Self {
            idx: 0,
            secs,
            ext_buf: Vec::with_capacity(1024),
            host: false,
            ext_oft: 0,
        }
    }
    // 把ext_buf中的数据拷贝到buf中
    fn copy_buf(&mut self, buf: &mut ReadBuf<'_>) {
        if self.ext_buf.len() > 0 {
            let left = self.ext_buf.len() - self.ext_oft;
            debug_assert!(left > 0);
            let n = left.min(buf.remaining());
            buf.put_slice(&self.ext_buf[self.ext_oft..self.ext_oft + n]);
            self.ext_oft += n;
            if self.ext_oft >= self.ext_buf.len() {
                self.ext_buf.clear();
                self.ext_oft = 0;
            }
        }
    }
}

use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

impl AsyncRead for Prometheus {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // 1. copy buffer
        self.copy_buf(buf);
        let metrics = crate::get_metrics();
        let len = metrics.len();
        let secs = self.secs;
        let Self {
            host, idx, ext_buf, ..
        } = &mut *self;
        // 2. write host metrics
        let mut w = PrometheusItemWriter::new(buf, ext_buf);
        if !*host {
            *host = true;
            HOST.try_lock().expect("host lock").snapshot(&mut w, secs);
        };
        // 3. write metrics by idx
        while w.remaining() > 0 && *idx < len {
            let (id, item) = metrics.get_item_id(*idx);
            item.snapshot(id, &mut w, secs);
            *idx += 1;
        }
        Poll::Ready(Ok(()))
    }
}

struct PrometheusItemWriter<'a, 'r, 'b> {
    left: &'b mut Vec<u8>,
    buf: &'a mut ReadBuf<'r>,
    first: bool, // 在lable中，第一个k/v前面不输出 ','
}
impl<'a, 'r, 'b> PrometheusItemWriter<'a, 'r, 'b> {
    fn new(buf: &'a mut ReadBuf<'r>, ext: &'b mut Vec<u8>) -> Self {
        Self {
            left: ext,
            buf,
            first: true,
        }
    }
    #[inline]
    fn remaining(&self) -> usize {
        self.buf.remaining()
    }
    #[inline]
    fn put_label(&mut self, name: &str, val: &[u8]) {
        if val.len() > 0 {
            //first 保证第一个label前没有 ","
            if !self.first {
                self.put_slice(b",");
            }
            self.put_slice(name.as_bytes());
            self.put_slice(b"=\"");
            self.put_slice(val);
            self.put_slice(b"\"");
            self.first = false;
        }
    }
}
impl<'a, 'r, 'b> ItemWriter for PrometheusItemWriter<'a, 'r, 'b> {
    #[inline]
    fn put_slice<S: AsRef<[u8]>>(&mut self, data: S) {
        let data = data.as_ref();
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
    fn write<V: WriteTo>(&mut self, name: &str, key: &str, sub_key: &str, val: V) {
        self.write_opts(name, key, sub_key, val, Vec::new());
    }
    fn write_opts<V: WriteTo>(
        &mut self,
        name: &str,
        key: &str,
        sub_key: &str,
        val: V,
        opts: Vec<(&str, &str)>,
    ) {
        /*
        四种类型:
              name                                              key         sub_key         result
        <1>   base                                              host        mem             host_mem{source="base",pool="default_pool"} 31375360
        <2>   mc_backend/ns1/127.0.0.1:8080                     timeout     qps             timeout_qps{source="mc_backend",namespace="ns",bip="127.0.0.1:8080",pool="default_pool"} 0
        <3>   mc.$namespace                                     $key        $sub_key        $key_$sub_key{source="mc",namespace="$namespace",pool="default_pool"} 0
        后端为mcq时，namespace 中包含 ‘#’分割字符, ‘#’ 前为namespace的值，‘#’ 后为topic的值，需要增加一个 topic lable，
        <4>   msgque_backend/msgque                             timeout     qps             timeout_qps{source="msgque_backend",pool="default_pool",namespace="ns",topic="top",bip="127.0.0.1:8080"} 0
         */

        //从 name 中截取 source、namespace和topic、instance
        let mut all_iter = name.split(crate::TARGET_SPLIT as char);
        let source = all_iter.next().unwrap_or("").as_bytes();
        let nameandtopic = all_iter.next().unwrap_or("");
        let bip = all_iter.next().unwrap_or("").as_bytes();
        //let charname = name.split(crate::TARGET_SPLIT as char).nth(1).unwrap_or("");
        //针对mcq,namespace中可能包含topic,先根据 ‘#’分割;
        let mut name_iter = nameandtopic.split("#");
        let namespace = name_iter.next().unwrap_or("").as_bytes();
        let topic = name_iter.next().unwrap_or("").as_bytes();

        let metric_name = MetricName(key, sub_key);

        //promethues # TYPE
        self.put_slice("# TYPE ");
        metric_name.write_to(self);
        self.put_slice(" gauge\n");

        //promethues metrics
        metric_name.write_to(self);
        self.put_slice("{");
        self.first = true;
        //确保第一个put的label一定不为空; 后续优化
        self.put_label("src", source);
        //self.put_label("pool", context::get().service_pool.as_bytes());
        self.put_label("ns", namespace);
        self.put_label("topic", topic);
        self.put_label("bip", bip);

        for (k, v) in opts {
            self.put_label(k, v.as_bytes());
        }

        self.put_slice("} ");

        //value
        val.write_to(self);
        self.put_slice("\n");
    }
}
use crate::Host;
use ds::lock::Lock;
use lazy_static::lazy_static;
lazy_static! {
    static ref HOST: Lock<Host> = Host::new().into();
}

struct MetricName<'a>(&'a str, &'a str);

impl<'a> WriteTo for MetricName<'a> {
    #[inline]
    fn write_to<W: ItemWriter>(&self, w: &mut W) {
        w.put_slice(self.0);
        if self.1.len() > 0 {
            w.put_slice("_");
            w.put_slice(self.1);
        }
    }
}
