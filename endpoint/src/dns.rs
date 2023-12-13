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
impl Backends for crate::uuid::config::UuidNamespace {
    fn get_backends(&self) -> &Vec<String> {
        &self.backends
    }
}
impl Backends for crate::phantomservice::config::PhantomNamespace {
    fn get_backends(&self) -> &Vec<String> {
        &self.backends
    }
}

impl Backends for crate::kv::config::KvNamespace {
    fn get_backends(&self) -> &Vec<String> {
        &self.backends_flaten
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
    pub fn check_load(&self, mut f: impl FnMut() -> bool) -> bool {
        // load之前，先把guard设置为false，这样避免在load的过程中，又有新的配置更新，导致丢失更新。
        if let Err(_e) = self.guard.compare_exchange(true, false, AcqRel, Relaxed) {
            log::warn!("{} clear_status failed", self._service);
        }
        if !f() {
            log::info!("{} load failed", self._service);
            if let Err(_e) = self.guard.compare_exchange(false, true, AcqRel, Relaxed) {
                log::warn!("{} renotified failed => {}", self._service, _e);
            }
            false
        } else {
            true
        }
    }
}

pub trait DnsLookup {
    // master_slave_mode:
    //      要求第一个元素是master，并且必须要解析出一个IP, 如果为多个，则选择第一个IP;
    //      slave: 至少为一个IP。可以为多个.
    // empty: 如果没有解析出IP，是否继续。
    // 如果所有的shard都没有解析出IP，则返回None，无论empty是否为true
    fn glookup<G: Lookup>(&self, master_slave_mode: bool, empty: bool) -> Option<Vec<Vec<String>>>;
    // 每个元素至少返回一个IP。即：每个element都不为空
    fn lookup(&self) -> Option<Vec<Vec<String>>> {
        self.glookup::<GLookup>(false, false)
    }
    // 每个元素的长度至少为2，第一个元素是master，之后的是slave
    // 如果master有多个IP，则选择最后一个IP
    fn master_lookup(&self) -> Option<Vec<Vec<String>>> {
        self.glookup::<GLookup>(true, false)
    }
    // 返回元素的长度至少为1。
    fn flatten_lookup(&self) -> Option<Vec<String>> {
        let mut all_ips = self.glookup::<GLookup>(false, true).unwrap_or_default();
        // 把所有的ip进行flatten
        let mut flatten_ips = Vec::with_capacity(all_ips.len());
        for ips in all_ips.iter_mut() {
            flatten_ips.append(ips);
        }
        (flatten_ips.len() > 0).then_some(flatten_ips)
    }
}

struct GLookup;
impl Lookup for GLookup {
    fn lookup(host: &str, f: impl FnMut(&[IpAddr])) {
        dns::lookup_ips(host, f);
    }
}

use std::net::IpAddr;
pub trait Lookup {
    fn lookup(host: &str, f: impl FnMut(&[IpAddr]));
}

impl DnsLookup for Vec<Vec<String>> {
    fn glookup<G: Lookup>(&self, master_slave_mode: bool, empty: bool) -> Option<Vec<Vec<String>>> {
        // 如果允许empty，则一定不能为master_slave_mode
        debug_assert!(!(master_slave_mode && empty));
        let mut all_ips = Vec::with_capacity(self.len());
        let mut total_ips = 0;
        for shard in self {
            let mut shard_ips = Vec::with_capacity(shard.len());
            // 第一个域名包含的ip的数量
            let mut first_ips_num = None;
            // 先把所有的ip解析出来。
            for url_port in shard {
                let host = url_port.host();
                let port = url_port.port();
                G::lookup(host, |ips| {
                    for ip in ips {
                        shard_ips.push(ip.to_string() + ":" + port);
                    }
                });
                first_ips_num.get_or_insert(shard_ips.len());
            }
            if !empty && shard_ips.len() == 0 {
                return None;
            }
            total_ips += shard_ips.len();
            let mut oft = 0;
            if master_slave_mode {
                let num = first_ips_num?;
                if num == 0 {
                    return None;
                }
                // 确保master至少有一个ip
                oft = num - 1;
                // 确保至少有一个slave
                if shard_ips.len() <= oft + 1 {
                    return None;
                }
            }
            all_ips.push(shard_ips.split_off(oft));
        }
        (total_ips > 0).then_some(all_ips)
    }
}
