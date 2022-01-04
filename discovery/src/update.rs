// 定期更新discovery.
use super::{Discover, ServiceId, TopologyWrite};
use crossbeam_channel::Receiver;
use std::time::{Duration, Instant};
use tokio::time::interval;

use crate::cache::DiscoveryCache;
use crate::path::ToName;
use std::collections::HashMap;

pub fn start_watch_discovery<D, T>(snapshot: &str, discovery: D, rx: Receiver<T>, tick: Duration)
where
    T: Send + TopologyWrite + ServiceId + 'static + Sync,
    D: Send + Sync + Discover + Unpin + 'static,
{
    let snapshot = snapshot.to_string();
    let cache = DiscoveryCache::new(discovery);
    tokio::spawn(async move {
        log::info!("discovery watch task started");
        let mut refresher = Refresher {
            snapshot: snapshot,
            discovery: cache,
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
    discovery: DiscoveryCache<D>,
    snapshot: String,
    tick: Duration,
    rx: Receiver<T>,
}

impl<D, T> Refresher<D, T>
where
    D: Discover + Send + Unpin + Sync,
    T: Send + TopologyWrite + ServiceId + 'static + Sync,
{
    async fn watch(&mut self) {
        // 降低tick的频率，便于快速从chann中接收新的服务。
        let mut tick = interval(Duration::from_secs(1));
        let mut services = HashMap::new();
        let mut last = Instant::now();
        loop {
            while let Ok(t) = self.rx.try_recv() {
                let service = t.service().name();
                if services.contains_key(&service) {
                    log::error!("service duplicatedly registered:{}", service);
                } else {
                    //t.name is 3+config+cloud+redis+testbreeze+onlytest:onlytest path 去掉冒号之前
                    log::info!("service {} path:{} registered ", t.name(), t.path());
                    let sig = if let Some((sig, _cfg)) = self.init(&mut t).await {
                        sig
                    } else {
                        String::new()
                    };
                    sigs.insert(t.name().to_string(), sig);
                    services.insert(t.name().to_string(), (t, Instant::now()));
                }
            }
            if last.elapsed() >= self.tick {
                self.check_once(&mut services).await;
                last = Instant::now();
            }
            for (_service, t) in services.iter_mut() {
                t.try_load();
            }
            tick.tick().await;
        }
    }
    // 从rx里面获取所有已注册的服务列表
    // 先从cache中取
    // 其次从remote获取
    async fn check_once(
        &mut self,
        services: &mut HashMap<String, (T, Instant)>,
        sigs: &mut HashMap<String, String>,
    ) {
        let mut cache: HashMap<String, (String, String)> = HashMap::with_capacity(services.len());
        for (name, (t, update)) in services.iter_mut() {
            let path = t.path().to_string();
            let sig = sigs.get_mut(name).expect("sig not inited");
            // 在某些场景下，同一个name被多个path共用。所以如果sig没有变更，则不需要额外处理更新。
            if let Some((cache_sig, cfg)) = cache.get(&path) {
                if cache_sig == sig {
                    continue;
                }
                if cfg.len() > 0 {
                    let res = t.resource();
                    let hosts = resource::parse_cfg_hosts(res, cfg).await;
                    t.update(name, &cfg, &hosts);
                    *update = Instant::now();
                    *sig = cache_sig.to_owned();
                    continue;
                }
            }
            let res = t.resource();
            let kindof_database = resource::name_kind(res);
            if let Some((remote_sig, cfg)) =
                self.load_from_discovery(&path, sig, kindof_database).await
            {
                let res = t.resource();
                let hosts = resource::parse_cfg_hosts(res, &cfg).await;
                t.update(name, &cfg, &hosts);
                *update = Instant::now();
                *sig = remote_sig.to_owned();
                cache.insert(path, (remote_sig, cfg));
            }
        }
    }
    // 先从snapshot加载，再从远程加载
    async fn init(&self, t: &mut T) -> Option<(String, String)> {
        let path = t.path().to_string();
        let name = t.name().to_string();
        let kindof_database = resource::name_kind(t.resource());
        println!("judge mc/redis{}", kindof_database);
        // 用path查找，用name更新。
        if let Ok((sig, cfg)) = self.try_load_from_snapshot(&path).await {
            let res = t.resource();
            let hosts = resource::parse_cfg_hosts(res, &cfg).await;
            t.update(&name, &cfg, &hosts);
            Some((sig, cfg))
        } else {
            //if let Some((sig, cfg)) = self.load_from_discovery(&path, "", path.to_owned()).await {
            if let Some((sig, cfg)) = self.load_from_discovery(&path, "", kindof_database).await {
                let res = t.resource();
                let hosts = resource::parse_cfg_hosts(res, &cfg).await;
                t.update(&name, &cfg, &hosts);
                Some((sig, cfg))
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
    async fn load_from_discovery(
        &self,
        path: &str,
        sig: &str,
        kindof_database: &str,
    ) -> Option<(String, String)> {
        use super::Config;
        match self
            .discovery
            .get_service::<String>(path, &sig, kindof_database)
            .await
        {
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
    fn _path(&self, name: &str) -> PathBuf {
        let base = name.replace("/", "+");
        let mut pb = PathBuf::new();
        pb.push(&self.snapshot);
        pb.push(base);
        pb
    }
    async fn dump_to_snapshot(&self, name: &str, sig: &str, cfg: &str) {
        log::info!("dump {} to snapshot. sig:{} cfg:{}", name, sig, cfg.len());
        //log::debug!("dump {} to snapshot. sig:{} cfg:{}", name, sig, cfg.len());
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
        // 清空缓存
        self.discovery.clear();
    }
}
