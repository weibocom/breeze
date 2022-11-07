// 定期更新discovery.
use super::{Discover, ServiceId, TopologyWrite};
use ds::chan::Receiver;
use ds::time::{Duration, Instant};
use tokio::time::interval;

use crate::cache::DiscoveryCache;
use crate::path::ToName;
use std::collections::HashMap;

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
unsafe impl<D, T> Send for Refresher<D, T> {}
unsafe impl<D, T> Sync for Refresher<D, T> {}

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
        let mut tick = interval(Duration::from_secs(1));
        self.cb.with_discovery(self.discovery.inner()).await;
        let mut services = HashMap::new();
        let mut last = Instant::now();
        loop {
            while let Ok(t) = self.rx.try_recv() {
                let service = t.service().name();
                if services.contains_key(&service) {
                    log::error!("service duplicatedly registered:{}", service);
                } else {
                    log::debug!("service path:{:?} registered ", service);
                    let mut t: crate::cfg::Config<T> = t.into();
                    t.init(&self.snapshot, &mut self.discovery).await;
                    services.insert(service, t);
                }
            }
            if last.elapsed() >= self.tick {
                self.check_once(&mut services).await;
                last = Instant::now();

                self.cb.with_discovery(self.discovery.inner()).await;
            }
            for (_service, t) in services.iter_mut() {
                t.try_load();
            }
            tick.tick().await;
        }
    }
    async fn check_once(&mut self, services: &mut HashMap<String, crate::cfg::Config<T>>) {
        for (_name, service) in services.iter_mut() {
            service
                .check_update(&self.snapshot, &mut self.discovery)
                .await;
        }
        // 清空缓存
        self.discovery.clear();
    }
}
