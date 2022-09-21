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
use std::time::{SystemTime, UNIX_EPOCH};
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
        //以毫秒为单位的时间戳
        let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();

        let metrics_name: String;
        let help_msg = "# HELP ".to_string();
        let type_msg = "# TYPE ".to_string();
        let gauge = "gauge".to_string();

        //主要3种结构(base/mc_backend/mc)
        if name == "base" {
            //添加 HELP && TYPE
            let base_metrics: String;
            if sub_key.len() > 0 {
                /*if sub_key.contains("-") {
                    let new_sub_key = &sub_key.replace("-", "_");
                    metrics_name = format!("{}_{}_{}",name,key,new_sub_key);
                } else {
                    metrics_name = format!("{}_{}_{}",name,key,sub_key);
                }*/
                metrics_name = format!("{}_{}_{}",name,key,sub_key);
                let base_help = format!("{}{} 平均耗时\n",help_msg,metrics_name);
                let base_type = format!("{}{} {}\n",type_msg,metrics_name,gauge);
                base_metrics = format!("{}{}{}{{pool=\"{}\",ip=\"{}\"}} {} {}\n\n", base_help,base_type,metrics_name,context::get().service_pool().clone(),super::ip::local_ip(),val,time);
            } else {
                metrics_name = format!("{}_{}",name,key);
                let base_help = format!("{}{} 平均耗时\n",help_msg,metrics_name);
                let base_type = format!("{}{} {}\n",type_msg,metrics_name,gauge);
                base_metrics = format!("{}{}{}{{pool=\"{}\",ip=\"{}\"}} {} {}\n\n", base_help,base_type,metrics_name,context::get().service_pool().clone(),super::ip::local_ip(),val,time);
            }

            self.put_slice(base_metrics.as_bytes());

        } else if name.contains("mc_backend") || name.contains("redis_backend") {
            //从 name 中截取 namespace、source、instance
            let source: String;
            let namespace: String;
            let instance: String;
            let backend: String;
            let backend_metrics: String;

            if name.contains("mc_backend") {
                source = "mc".to_string();
                backend = "mc_backend".to_string();
            } else {
                source = "redis".to_string();
                backend = "redis_backend".to_string();
            }

            let nsandins = &name[backend.len()+1..];
            let index = nsandins.find(".").unwrap();

            namespace = nsandins[0..index].to_string();
            instance = nsandins[index+1..].to_string();

            if sub_key.len() > 0 {
                metrics_name = format!("backend_{}_{}",key,sub_key);
                let backend_help = format!("{}{} 平均耗时\n",help_msg,metrics_name);
                let backend_type = format!("{}{} {}\n",type_msg,metrics_name,gauge);
                backend_metrics = format!("{}{}{}{{pool=\"{}\",ip=\"{}\",namespace=\"{}\",source=\"{}\",instance=\"{}\"}} {} {}\n\n",
                backend_help,backend_type,metrics_name,context::get().service_pool().clone(),super::ip::local_ip(),namespace,source,instance,val,time);
            } else {
                metrics_name = format!("backend_{}",key);
                let backend_help = format!("{}{} 平均耗时\n",help_msg,metrics_name);
                let backend_type = format!("{}{} {}\n",type_msg,metrics_name,gauge);
                backend_metrics = format!("{}{}{}{{pool=\"{}\",ip=\"{}\",namespace=\"{}\",source=\"{}\",instance=\"{}\"}} {} {}\n\n",
                backend_help,backend_type,metrics_name,context::get().service_pool().clone(),super::ip::local_ip(),namespace,source,instance,val,time);
            }

            self.put_slice(backend_metrics.as_bytes());

        } else {

            let source: String;
            let namespace: String;
            let source_metrics: String;

            if name.contains("mc") {
                source = "mc".to_string();
            } else {
                source = "redis".to_string();
            }
            namespace = name[source.len()+1..].to_string();
            if sub_key.len() > 0 {
                metrics_name = format!("source_{}_{}",key,sub_key);
                let source_help = format!("{}{} 平均耗时\n",help_msg,metrics_name);
                let source_type = format!("{}{} {}\n",type_msg,metrics_name,gauge);
                source_metrics = format!("{}{}{}{{pool=\"{}\",ip=\"{}\",namespace=\"{}\",source=\"{}\"}} {} {}\n\n",
                source_help,source_type,metrics_name,context::get().service_pool().clone(),super::ip::local_ip(),namespace,source,val,time);
            } else {
                metrics_name = format!("source_{}",key);
                let source_help = format!("{}{} 平均耗时\n",help_msg,metrics_name);
                let source_type = format!("{}{} {}\n",type_msg,metrics_name,gauge);
                source_metrics = format!("{}{}{}{{pool=\"{}\",ip=\"{}\",namespace=\"{}\",source=\"{}\",}} {} {}\n\n",
                source_help,source_type,metrics_name,context::get().service_pool().clone(),super::ip::local_ip(),namespace,source,val,time);
            }
            self.put_slice(source_metrics.as_bytes());
        }
    }
}
use crate::Host;
use ds::lock::Lock;
use lazy_static::lazy_static;
lazy_static! {
    static ref HOST: Lock<Host> = Host::new().into();
}
