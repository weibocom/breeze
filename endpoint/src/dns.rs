use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering::*};
use std::sync::Arc;

use discovery::dns::{self, IPPort};

#[derive(Debug, Clone)]
pub(crate) struct DnsConfig<T> {
    pub(crate) config: T,
    pub(crate) shards_url: Vec<Vec<String>>,
    pub(crate) service: String,
    pub(crate) updated: HashMap<String, Arc<AtomicBool>>,
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
        self.updated
            .get_mut(CONFIG_UPDATED_KEY)
            .expect("config updated key not found")
            .store(true, Release);
    }
    fn register(&mut self) {
        self.shards_url.iter().for_each(|replicas| {
            replicas.iter().for_each(|url_port| {
                let host = url_port.host();
                if !self.updated.contains_key(host) {
                    let watcher = dns::register(host);
                    self.updated.insert(host.to_string(), watcher);
                }
            })
        });
    }
    pub fn need_load(&self) -> bool {
        self.updated
            .iter()
            .fold(false, |acc, (_k, v)| acc || v.load(Acquire))
    }
    pub fn clear_status(&mut self) {
        for (_, updated) in self.updated.iter() {
            updated.store(false, Release);
        }
    }
    pub fn enable_notified(&mut self) {
        self.updated
            .get_mut(CONFIG_UPDATED_KEY)
            .expect("config state missed")
            .store(true, Release);
    }
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

const CONFIG_UPDATED_KEY: &str = "__config__";
impl<T: Default> Default for DnsConfig<T> {
    fn default() -> Self {
        let mut me = DnsConfig {
            config: T::default(),
            shards_url: Vec::new(),
            service: String::new(),
            updated: HashMap::new(),
        };
        me.updated.insert(
            CONFIG_UPDATED_KEY.to_string(),
            Arc::new(AtomicBool::new(false)),
        );
        me
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
