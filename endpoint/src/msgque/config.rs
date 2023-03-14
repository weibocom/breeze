use std::{collections::BTreeMap, time::Duration};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Namespace {
    // TODO loadbalance 稳定后去掉，不需要业务设置
    #[serde(default)]
    pub loadbalance: String, // eg: weight

    #[serde(default)]
    pub resource_type: String, //eg: mcq2,mcq3

    // 下线的ip列表，只可读，不可写
    #[serde(default)]
    pub offline: Vec<String>,

    // 线上的que，可直接读写
    #[serde(default)]
    pub que_512: Vec<String>,
    #[serde(default)]
    pub que_1024: Vec<String>,
    #[serde(default)]
    pub que_2048: Vec<String>,
    #[serde(default)]
    pub que_4096: Vec<String>,
    #[serde(default)]
    pub que_8192: Vec<String>,
    #[serde(default)]
    pub que_16384: Vec<String>,
    #[serde(default)]
    pub que_32768: Vec<String>,

    // Map<size, Vec<ip>>，key：存放的msg最大size；value：该size的队列ip列表
    #[serde(default)]
    pub sized_queue: BTreeMap<usize, Vec<String>>,

    #[serde(default)]
    pub(crate) timeout_read: u32,
    #[serde(default)]
    pub(crate) timeout_write: u32,

    #[serde(default)]
    pub offline_idle_time: Duration,
}

impl Namespace {
    pub(crate) fn try_from(cfg: &str, _namespace: &str) -> Option<Self> {
        log::debug!("mq/{} parsing - cfg: {}", _namespace, cfg);
        match serde_yaml::from_str::<Namespace>(cfg) {
            Err(_e) => {
                log::warn!("parse ns/{} failed: {:?}, cfg: {}", _namespace, _e, cfg);
                return None;
            }
            Ok(mut ns) => {
                ns.collect_queue(512, ns.que_512.clone());
                ns.collect_queue(1024, ns.que_1024.clone());
                ns.collect_queue(2048, ns.que_2048.clone());
                ns.collect_queue(4096, ns.que_4096.clone());
                ns.collect_queue(8192, ns.que_8192.clone());
                ns.collect_queue(16384, ns.que_16384.clone());
                ns.collect_queue(32768, ns.que_32768.clone());

                if ns.sized_queue.len() == 0 {
                    log::warn!("no queue for {}: {}", _namespace, cfg);
                    return None;
                }

                // offline后读空的时间至少是5分钟
                let min_idle = 5 * 60;
                if ns.offline_idle_time.as_secs() < min_idle {
                    ns.offline_idle_time = Duration::from_secs(min_idle);
                }

                return Some(ns);
            }
        }
    }

    #[inline]
    fn collect_queue(&mut self, size: usize, que: Vec<String>) {
        log::debug!("+++ size:{}, que:{:?}", size, que);
        if que.len() > 0 {
            self.sized_queue.insert(size, que);
        }
    }

    //#[inline]
    //pub(crate) fn timeout_read(&self) -> Duration {
    //    Duration::from_millis(200.max(self.timeout_read))
    //}

    //#[inline]
    //pub(crate) fn timeout_write(&self) -> Duration {
    //    Duration::from_millis(300.max(self.timeout_write))
    //}
}
