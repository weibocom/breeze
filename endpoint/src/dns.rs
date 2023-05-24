use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering::*};
use std::sync::Arc;

use discovery::dns::{self, IPPort};

#[derive(Debug, Clone, Default)]
pub(crate) struct DnsConfig<T> {
    pub(crate) config: T,
    pub(crate) shards_url: Vec<Vec<String>>,
    pub(crate) service: String,
    pub(crate) updated: Arc<AtomicBool>,
    registered: HashMap<String, ()>,
}
impl<T: Backends> DnsConfig<T> {
    fn build_shards_url(&mut self) {
        let shards_url: Vec<Vec<String>> = self
            .config
            .get_backends()
            .iter()
            .map(|shard| shard.split(",").map(|s| s.to_string()).collect())
            .collect();
        self.shards_url = shards_url;
    }
    pub fn update(&mut self, service: &str, cfg: T) {
        self.config = cfg;
        self.service = service.to_string();
        self.build_shards_url();
        self.register();
        // 注册通知

        // 配置更新完毕，如果watcher确认配置update了，各个topo就重新进行load
        self.updated.store(true, Relaxed);
    }
    fn register(&mut self) {
        self.shards_url.iter().for_each(|replicas| {
            replicas.iter().for_each(|url_port| {
                let host = url_port.host();
                if !self.registered.contains_key(host) {
                    dns::register(host, self.updated.clone());
                    self.registered.insert(host.to_string(), ());
                }
            })
        });
    }
    pub fn need_load(&self) -> bool {
        self.updated.load(Relaxed)
    }
    pub fn load_guard(&self) -> LoadGuard {
        LoadGuard {
            _service: self.service.to_string(),
            guard: self.updated.clone(),
        }
    }
    // 把updated设置为false，这样topo就不会重新load
    //pub fn clear_status(&mut self) {
    //    if let Err(_e) = self.updated.compare_exchange(true, false, AcqRel, Relaxed) {
    //        log::warn!("{} clear_status failed", self.service,);
    //    }
    //}
    //// 把updated设置为true，这样topo就会重新load
    //pub fn enable_notified(&mut self) {
    //    if let Err(_e) = self.updated.compare_exchange(false, true, AcqRel, Relaxed) {
    //        log::warn!("{} clear_status failed", self.service,);
    //    }
    //}
}

pub(crate) trait Backends {
    fn get_backends(&self) -> &Vec<String>;
}

impl Backends for crate::redisservice::config::RedisNamespace {
    fn get_backends(&self) -> &Vec<String> {
        &self.backends
    }
}
impl Backends for crate::phantomservice::config::PhantomNamespace {
    fn get_backends(&self) -> &Vec<String> {
        &self.backends
    }
}

impl Backends for crate::kv::config::MysqlNamespace {
    fn get_backends(&self) -> &Vec<String> {
        &self.backends_url
    }
}

impl<T> std::ops::Deref for DnsConfig<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.config
    }
}
impl<T> std::ops::DerefMut for DnsConfig<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.config
    }
}

pub struct LoadGuard {
    _service: String,
    guard: Arc<AtomicBool>,
}

impl LoadGuard {
    pub fn check_load(&self, mut f: impl FnMut() -> bool) {
        // load之前，先把guard设置为false，这样避免在load的过程中，又有新的配置更新，导致丢失更新。
        if let Err(_e) = self.guard.compare_exchange(true, false, AcqRel, Relaxed) {
            log::warn!("{} clear_status failed", self._service);
        }
        if !f() {
            log::info!("{} load failed", self._service);
            if let Err(_e) = self.guard.compare_exchange(false, true, AcqRel, Relaxed) {
                log::warn!("{} renotified failed => {}", self._service, _e);
            }
        }
    }
}
