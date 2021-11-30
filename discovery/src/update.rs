use crate::resource;

// 定期更新discovery.
use super::{Discover, ServiceId, TopologyWrite};
use crossbeam_channel::Receiver;
use ds::DnsResolver;
use protocol::Resource;
use std::io::{Error, ErrorKind, Result};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::interval;

use std::collections::{HashMap, HashSet};

pub fn start_watch_discovery<D, T>(snapshot: &str, discovery: D, rx: Receiver<T>, tick: Duration)
where
    T: Send + TopologyWrite + ServiceId + 'static,
    D: Send + Sync + Discover + Unpin + 'static,
{
    let snapshot = snapshot.to_string();
    tokio::spawn(async move {
        log::info!("discovery watch task started");
        let mut refresher = Refresher {
            snapshot: snapshot,
            discovery: discovery,
            rx: rx,
            tick: tick,
        };
        refresher.watch().await;
        log::info!("discovery watch task complete");
    });
}
unsafe impl<D, T> Send for Refresher<D, T> {}
unsafe impl<D, T> Sync for Refresher<D, T> {}

struct Refresher<D, T> {
    discovery: D,
    snapshot: String,
    tick: Duration,
    rx: Receiver<T>,
}

impl<D, T> Refresher<D, T>
where
    D: Discover + Send + Unpin,
    T: Send + TopologyWrite + ServiceId + 'static,
{
    async fn watch(&mut self) {
        // 降低tick的频率，便于快速从chann中接收新的服务。
        let mut tick = interval(Duration::from_secs(1));
        let mut services = HashMap::new();
        let mut sigs = HashMap::new();
        let mut last_checked_cfg = Instant::now();
        let mut last_checked_hosts = Instant::now();
        let mut rd_sec = 50 + rand::random::<u64>() % 20;

        // 所有service上一次请求配置所对应的host及ips
        let mut last_hosts: HashMap<String, HashMap<String, HashSet<String>>> =
            HashMap::with_capacity(10);
        let dns_resolver = DnsResolver::with_sysy_conf();
        loop {
            while let Ok(mut t) = self.rx.try_recv() {
                if services.contains_key(t.name()) {
                    log::error!("service duplicatedly registered:{}", t.name());
                } else {
                    log::info!("service {} path:{} registered ", t.name(), t.path());
                    let sig = if let Some((sig, _cfg)) =
                        self.init(&dns_resolver, &mut t, &mut last_hosts).await
                    {
                        sig
                    } else {
                        String::new()
                    };
                    sigs.insert(t.name().to_string(), sig);
                    services.insert(t.name().to_string(), (t, Instant::now()));
                }
            }
            if last_checked_cfg.elapsed() >= self.tick {
                self.check_once(&dns_resolver, &mut services, &mut sigs, &mut last_hosts)
                    .await;
                last_checked_cfg = Instant::now();
            }

            // check hosts，每50-70秒左右进行一次全量探测
            if last_checked_hosts.elapsed() >= Duration::from_secs(rd_sec) {
                log::debug!("checking hosts....");
                rd_sec = 50 + rand::random::<u64>() % 20;
                self.update_topo_hosts(&dns_resolver, &mut services, &mut last_hosts)
                    .await;
                log::debug!("checked hosts....");
                last_checked_hosts = Instant::now();
            }

            // gc
            for (_, (t, update)) in services.iter_mut() {
                // 一次topo更新后，在stream::io::Monitor里面会在255秒钟内更新逻辑连接。
                // 因此，300秒后开始进行gc
                if update.elapsed() >= Duration::from_secs(300) {
                    t.gc();
                    *update = Instant::now();
                }
            }
            tick.tick().await;
        }
    }
    // 从rx里面获取所有已注册的服务列表
    // 先从cache中取
    // 其次从remote获取
    // 如果配置不变，还check host对应ip并更新
    async fn check_once(
        &mut self,
        dns_resolver: &DnsResolver,
        services: &mut HashMap<String, (T, Instant)>,
        sigs: &mut HashMap<String, String>,
        last_hosts: &mut HashMap<String, HashMap<String, HashSet<String>>>,
    ) {
        let mut cache: HashMap<String, (String, String)> = HashMap::with_capacity(services.len());
        for (name, (t, update)) in services.iter_mut() {
            let path = t.path().to_string();
            let sig = sigs.get_mut(name).expect("sig not inited");
            // 在某些场景下，同一个path被多个name共用。所以如果sig没有变更，则不需要额外处理更新。
            if let Some((cache_sig, cfg)) = cache.get(&path) {
                if cache_sig == sig {
                    continue;
                }
                if cfg.len() > 0 {
                    if self.update_topo(&dns_resolver, t, cfg, last_hosts).await {
                        // 更新topo成功后，才会变更sign值
                        *update = Instant::now();
                        *sig = cache_sig.to_owned();
                    }
                    continue;
                }
            }

            if let Some((remote_sig, cfg)) = self.load_from_discovery(&path, sig).await {
                if self.update_topo(&dns_resolver, t, &cfg, last_hosts).await {
                    // 更新topo成功后，才会变更sign值
                    *update = Instant::now();
                    *sig = remote_sig.to_owned();
                    cache.insert(path, (remote_sig, cfg));
                }
            }
        }
    }

    async fn update_topo(
        &self,
        dns_resolver: &DnsResolver,
        t: &mut T,
        cfg: &str,
        last_hosts: &mut HashMap<String, HashMap<String, HashSet<String>>>,
    ) -> bool {
        let res = t.resource();
        let name = t.name().to_string();
        match resource::parse_cfg_hosts(dns_resolver, &res, cfg).await {
            Ok(hosts) => {
                log::debug!("updating topo/{} cfg...", name);
                t.update(name.as_str(), &cfg, &hosts);
                last_hosts.insert(name, hosts);
                return true;
            }
            Err(e) => {
                log::warn!("updated topo/{} failed: {:?}", name, e);
                return false;
            }
        }
    }

    // 更新hosts的ip列表，对于业务topo，只有所有hosts都解析成功，才更新，否则忽略等下次
    async fn update_topo_hosts(
        &self,
        dns_resolver: &DnsResolver,
        services: &mut HashMap<String, (T, Instant)>,
        last_hosts: &mut HashMap<String, HashMap<String, HashSet<String>>>,
    ) {
        let empty_hosts = HashMap::with_capacity(0);
        for (name, (t, _)) in services.iter_mut() {
            if !self.need_check_hosts(&t.resource()) {
                continue;
            }

            let old = last_hosts.get(name).unwrap_or(&empty_hosts);
            if old.len() == 0 {
                log::warn!("{} has no hosts", name);
                return;
            }
            // 获取host对应的ips，check是否发生变化
            let mut h = Vec::with_capacity(old.len());
            h.extend(old.keys().clone());
            // hosts全部parse成功，才会更新，否则不更新
            match resource::lookup_hosts(&dns_resolver, h).await {
                Ok(new) => {
                    if new.eq(old) {
                        return;
                    }
                    if new.len() > 0 {
                        log::debug!("update {}'s hosts, old: {:?}, new: {:?}", name, old, new);
                        t.update_hosts(&name, &new);
                        last_hosts.insert(name.clone(), new);
                    }
                }
                Err(e) => log::warn!("ignore updating {}'s hosts for: {:?}", name, e),
            }
        }
    }

    // 先从snapshot加载，再从远程加载
    async fn init(
        &self,
        dns_resolver: &DnsResolver,
        t: &mut T,
        last_hosts: &mut HashMap<String, HashMap<String, HashSet<String>>>,
    ) -> Option<(String, String)> {
        let path = t.path().to_string();
        // 用path查找，用name更新。
        if let Ok((sig, cfg)) = self.try_load_from_snapshot(&path).await {
            if self.update_topo(dns_resolver, t, &cfg, last_hosts).await {
                return Some((sig, cfg));
            }
            return None;
        } else {
            if let Some((sig, cfg)) = self.load_from_discovery(&path, "").await {
                if self.update_topo(dns_resolver, t, &cfg, last_hosts).await {
                    return Some((sig, cfg));
                }
                return None;
            } else {
                None
            }
        }
    }
    async fn try_load_from_snapshot(&self, name: &str) -> Result<(String, String)> {
        let mut contents = Vec::with_capacity(8 * 1024);
        File::open(&self._path(name))
            .await?
            .read_to_end(&mut contents)
            .await?;
        let mut contents = String::from_utf8(contents)
            .map_err(|_e| Error::new(ErrorKind::Other, "not a valid utfi file"))?;
        // 内容的第一行是签名，第二行是往后是配置
        let idx = contents.find('\n').unwrap_or(0);
        let cfg = contents.split_off(idx);
        let sig = contents;
        log::info!("{} snapshot loaded:sig:{} cfg:{}", name, sig, cfg.len());
        Ok((sig, cfg))
    }
    async fn load_from_discovery(&self, path: &str, sig: &str) -> Option<(String, String)> {
        use super::Config;
        match self.discovery.get_service::<String>(path, &sig).await {
            Err(e) => {
                log::warn!("load service topology failed. path:{}, e:{:?}", path, e);
            }
            Ok(c) => match c {
                Config::NotChanged => {}
                Config::NotFound => {
                    log::info!("{} not found in discovery", path);
                }
                Config::Config(sig, cfg) => {
                    self.dump_to_snapshot(path, &sig, &cfg).await;
                    return Some((sig, cfg));
                }
            },
        }
        None
    }
    // 是否需要检查业务topo的hosts，目前只有redis才需要
    fn need_check_hosts(&self, resource: &Resource) -> bool {
        match resource {
            Resource::Redis => return true,
            _ => return false,
        }
    }
    fn _path(&self, name: &str) -> PathBuf {
        let base = name.replace("/", "+");
        let mut pb = PathBuf::new();
        pb.push(&self.snapshot);
        pb.push(base);
        pb
    }
    async fn dump_to_snapshot(&self, name: &str, sig: &str, cfg: &str) {
        log::debug!("dump {} to snapshot. sig:{} cfg:{}", name, sig, cfg.len());
        match self.try_dump_to_snapshot(name, sig, cfg).await {
            Ok(_) => {}
            Err(e) => {
                log::warn!(
                    "failed to dump (name:{} sig:{}) cfg len:{} to snapshot err:{:?}",
                    name,
                    sig,
                    cfg.len(),
                    e
                )
            }
        }
    }
    async fn try_dump_to_snapshot(&self, name: &str, sig: &str, cfg: &str) -> Result<()> {
        let path = self._path(name);
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                tokio::fs::create_dir_all(parent).await?;
            }
        }
        let mut file = File::create(path).await?;
        file.write_all(sig.as_bytes()).await?;
        file.write_all(b"\n").await?;
        file.write_all(cfg.as_bytes()).await?;
        Ok(())
    }
}
