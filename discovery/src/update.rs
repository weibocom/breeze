// 定期更新discovery.
use super::{Discover, ServiceId, TopologyWrite};
use ds::chan::Receiver;
use ds::time::{interval, Duration};

use crate::cache::DiscoveryCache;
use crate::path::ToName;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering::*};

pub async fn watch_discovery<D, T>(
    snapshot: String,
    discovery: D,
    rx: Receiver<T>,
    tick: Duration,
    cb: super::fixed::Fixed,
) where
    T: Send + TopologyWrite + ServiceId + 'static + Sync,
    D: Send + Sync + Discover + Unpin + 'static,
{
    let tick = Duration::from_secs(3).max(tick);
    let cache = DiscoveryCache::new(discovery);
    let mut refresher = Refresher {
        snapshot,
        discovery: cache,
        rx,
        tick,
        cb,
    };
    refresher.watch().await
}

struct Refresher<D, T> {
    discovery: DiscoveryCache<D>,
    snapshot: String,
    tick: Duration,
    rx: Receiver<T>,
    cb: super::fixed::Fixed,
}

impl<D, T> Refresher<D, T>
where
    D: Discover + Send + Unpin + Sync,
    T: Send + TopologyWrite + ServiceId + 'static + Sync,
{
    async fn watch(&mut self) {
        log::info!("task started ==> topology refresher");
        // 降低tick的频率，便于快速从chann中接收新的服务。
        let period = Duration::from_secs(1);
        let cycle = (self.tick.as_secs_f64() / period.as_secs_f64()).ceil() as usize;
        let mut cycle_i = 0usize;
        let mut first_cycle = true;
        let mut tick = interval(period);
        self.cb.with_discovery(self.discovery.inner()).await;
        let mut services = HashMap::new();
        // 每秒钟处理一次，一次只处理部分service。
        // 每个周期结束清理缓存
        loop {
            while let Ok(t) = self.rx.try_recv() {
                let service = t.service().name();
                if services.contains_key(&service) {
                    log::error!("service duplicatedly registered:{}", service);
                } else {
                    static SEQ: AtomicUsize = AtomicUsize::new(0);
                    let id = SEQ.fetch_add(1, Relaxed);
                    log::debug!("service path:{:?} registered ", service);
                    let mut t: crate::cfg::Config<T> = t.into();
                    t.init(&self.snapshot, &mut self.discovery).await;
                    services.insert(service, (id, t));
                }
            }
            for (_name, (id, service)) in services.iter_mut() {
                // 每秒钟只更新部分。
                if cycle_i == *id % cycle {
                    service
                        .check_update(&self.snapshot, &mut self.discovery)
                        .await;
                }
            }

            for (_service, (_id, t)) in services.iter_mut() {
                t.try_load();
            }
            cycle_i += 1;
            if cycle_i == cycle {
                self.cb.with_discovery(self.discovery.inner()).await;
                cycle_i = 0;
                // 清空缓存
                self.discovery.clear();
            }
            // 第一个循环处于冷启动期，需要加快固定任务的加载频率，避免请求失败导致其他问题(如socks)
            if first_cycle && !self.cb.inited() {
                self.cb.with_discovery(self.discovery.inner()).await;
                first_cycle = false;
            }
            tick.tick().await;
        }
    }
}
