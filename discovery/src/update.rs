// 定期更新discovery.
use super::{Discover, ServiceId, TopologyWrite};
use crossbeam_channel::Receiver;
use std::io::{Error, ErrorKind, Result};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::interval_at;

use std::collections::HashMap;

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
        let start = Instant::now() + Duration::from_secs(1);
        let mut tick = interval_at(start.into(), self.tick);
        let mut services = HashMap::new();
        let mut sigs = HashMap::new();
        loop {
            while let Ok(t) = self.rx.try_recv() {
                log::info!("service {} registered, interval {:?}", t.name(), self.tick);
                services.insert(t.name().to_string(), t);
            }
            self.check_once(&mut services, &mut sigs).await;
            tick.tick().await;
        }
    }
    // 从rx里面获取所有已注册的服务列表
    // 优先从snaphost里面load
    // 其次从remove获取
    async fn check_once(
        &mut self,
        services: &mut HashMap<String, T>,
        sigs: &mut HashMap<String, String>,
    ) {
        let mut cache: HashMap<String, (String, String)> = HashMap::with_capacity(services.len());
        for (name, t) in services.iter_mut() {
            let path = t.path().to_string();
            let empty = String::new();
            let sig = sigs.get(name).unwrap_or(&empty);
            // 在某些场景下，同一个name被多个path共用。所以如果sig没有变更，则不需要额外处理更新。
            if let Some((path_sig, cfg)) = cache.get(&path) {
                if path_sig == sig {
                    continue;
                }
                if cfg.len() > 0 {
                    t.update(name, &cfg);
                    continue;
                }
            }

            // 尝试优先加载本地的snapshot
            if !sigs.contains_key(name) {
                if let Ok((sig, cfg)) = self.try_load_from_snapshot(&name).await {
                    sigs.insert(name.to_string(), sig);
                    t.update(name, &cfg);
                    continue;
                }
            }
            match self.load_from_discovery(&path, sig).await {
                Err(e) => log::warn!("failed to load service config '{}' err:{:?}", name, e),
                Ok(Some((sig, cfg))) => {
                    self.dump_to_snapshot(name, &sig, &cfg).await;
                    sigs.insert(name.to_string(), sig.to_string());
                    t.update(name, &cfg);
                    cache.insert(path, (sig, cfg));
                }
                _ => {
                    cache.insert(path, (sig.to_string(), empty));
                }
            }
        }
    }
    async fn try_load_from_snapshot(&self, name: &str) -> Result<(String, String)> {
        let mut contents = Vec::with_capacity(8 * 1024);
        File::open(&self.path(name))
            .await?
            .read_to_end(&mut contents)
            .await?;
        let mut contents = String::from_utf8(contents)
            .map_err(|_e| Error::new(ErrorKind::Other, "not a valid utfi file"))?;
        // 内容的第一行是签名，第二行是往后是配置
        let idx = contents.find('\n').unwrap_or(0);
        let cfg = contents.split_off(idx);
        let sig = contents;
        log::info!("snapshot loaded: sig:{} cfg:{}", sig.len(), cfg.len());
        Ok((sig, cfg))
    }
    async fn load_from_discovery(&self, name: &str, sig: &str) -> Result<Option<(String, String)>> {
        use super::Config;
        match self.discovery.get_service(name, &sig).await? {
            Config::NotChanged => Ok(None),
            Config::NotFound => Err(Error::new(
                ErrorKind::NotFound,
                format!("service not found. name:{}", name),
            )),
            Config::Config(sig, cfg) => Ok(Some((sig, cfg))),
        }
    }
    fn path(&self, name: &str) -> PathBuf {
        let mut pb = PathBuf::new();
        pb.push(&self.snapshot);
        pb.push(name);
        pb
    }
    async fn dump_to_snapshot(&mut self, name: &str, sig: &str, cfg: &str) {
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
    async fn try_dump_to_snapshot(&mut self, name: &str, sig: &str, cfg: &str) -> Result<()> {
        let path = self.path(name);
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                tokio::fs::create_dir_all(parent).await?;
            }
        }
        let mut file = File::create(path).await?;
        file.write_all(sig.as_bytes()).await?;
        file.write_all(cfg.as_bytes()).await?;
        Ok(())
    }
}

//pub(crate) struct AsyncServiceUpdate<D, T> {
//    service: ServiceName,
//    discovery: D,
//    w: Arc<Spmc<T>>,
//    cfg: String,
//    sign: String,
//    interval: Interval,
//    snapshot: PathBuf, // 本地快照存储的文件名称
//}
//
//impl<D, T> AsyncServiceUpdate<D, T>
//where
//    T: super::TopologyWrite,
//{
//    pub fn new(
//        service: String,
//        discovery: D,
//        writer: Arc<Spmc<T>>,
//        tick: Duration,
//        snapshot: String,
//    ) -> Self {
//        let snapshot = if snapshot.len() == 0 {
//            "/tmp/breeze/services/snapshot".to_owned()
//        } else {
//            snapshot
//        };
//        let mut path = PathBuf::from(snapshot);
//        path.push(&service);
//        Self {
//            service: ServiceName::from(service),
//            discovery: discovery,
//            w: writer,
//            cfg: Default::default(),
//            sign: Default::default(),
//            interval: interval(tick),
//            snapshot: path,
//        }
//    }
//    fn path(&self) -> PathBuf {
//        let mut pb = PathBuf::new();
//        pb.push(&self.snapshot);
//        pb.push(self.service.name());
//        pb
//    }
//    // 如果当前配置为空，则从snapshot加载
//    async fn load_from_snapshot(&mut self) -> Result<()>
//    where
//        D: Discover + Send + Unpin,
//    {
//        if self.cfg.len() != 0 {
//            return Ok(());
//        }
//        let mut contents = vec![];
//        File::open(&self.path())
//            .await?
//            .read_to_end(&mut contents)
//            .await?;
//        let mut contents = String::from_utf8(contents)
//            .map_err(|_e| Error::new(ErrorKind::Other, "not a valid utfi file"))?;
//        // 内容的第一行是签名，第二行是往后是配置
//        let idx = contents.find('\n').unwrap_or(0);
//        let cfg = contents.split_off(idx);
//        self.cfg = cfg;
//        self.sign = contents;
//
//        self.update();
//
//        Ok(())
//    }
//    fn update(&mut self) {
//        self.w.write(|t| {
//            t.update(&self.cfg, &self.service);
//        });
//    }
//    async fn dump_to_snapshot(&mut self) -> Result<()> {
//        let path = self.snapshot.as_path();
//        if let Some(parent) = path.parent() {
//            if !parent.exists() {
//                tokio::fs::create_dir_all(parent).await?;
//            }
//        }
//        let mut file = File::create(path).await?;
//        file.write_all(self.sign.as_bytes()).await?;
//        file.write_all(self.cfg.as_bytes()).await?;
//        Ok(())
//    }
//    async fn load_from_discovery(&mut self) -> Result<()>
//    where
//        D: Discover + Send + Unpin,
//    {
//        use super::Config;
//        match self
//            .discovery
//            .get_service(&self.service, &self.sign)
//            .await?
//        {
//            Config::NotChanged => Ok(()),
//            Config::NotFound => Err(Error::new(
//                ErrorKind::NotFound,
//                format!("service not found. name:{}", self.service.name()),
//            )),
//            Config::Config(cfg, sig) => {
//                if self.cfg != cfg || self.sign != sig {
//                    self.cfg = cfg;
//                    self.sign = sig;
//                    self.update();
//                    if let Err(e) = self.dump_to_snapshot().await {
//                        log::warn!(
//                            "failed to dump to snapshot. path:{:?} {:?}",
//                            self.snapshot,
//                            e
//                        );
//                    };
//                }
//                Ok(())
//            }
//        }
//    }
//    pub async fn start_watch(&mut self)
//    where
//        D: Discover + Send + Unpin,
//    {
//        let _r = self.load_from_snapshot().await;
//        if self.cfg.len() == 0 {
//            let r = self.load_from_discovery().await;
//            self.check(r);
//        }
//        loop {
//            self.interval.tick().await;
//            let r = self.load_from_discovery().await;
//            self.check(r);
//        }
//    }
//    fn check(&self, r: Result<()>) {
//        match r {
//            Ok(_) => {}
//            Err(e) => {
//                log::warn!(
//                    "load service config error. service:{} error:{:?}",
//                    self.service.name(),
//                    e
//                );
//            }
//        }
//    }
//}
