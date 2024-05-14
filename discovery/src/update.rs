use metrics::Metric;
// 定期更新discovery.
use super::{Discover, TopologyWrite};
use ds::chan::Receiver;
use ds::time::{interval, Duration};

use std::collections::HashMap;

pub async fn watch_discovery<D, T>(
    snapshot: String,
    discovery: D,
    rx: Receiver<(String, T)>,
    tick: Duration,
    cb: super::fixed::Fixed,
) where
    T: Send + TopologyWrite + 'static + Sync,
    D: Send + Sync + Discover + Unpin + 'static,
{
    let tick = Duration::from_secs(3).max(tick);
    let mut refresher = Refresher {
        snapshot,
        discovery,
        rx,
        tick,
        cb,
    };
    refresher.watch().await
}

struct Refresher<D, T> {
    discovery: D,
    snapshot: String,
    tick: Duration,
    rx: Receiver<(String, T)>,
    cb: super::fixed::Fixed,
}

impl<D, T> Refresher<D, T>
where
    D: Discover + Send + Unpin + Sync,
    T: Send + TopologyWrite + 'static + Sync,
{
    async fn watch(&mut self) {
        log::info!("task started ==> topology refresher");
        // 降低tick的频率，便于快速从chann中接收新的服务。
        let period = Duration::from_secs(1);
        let cycle = (self.tick.as_secs_f64() / period.as_secs_f64()).ceil() as usize;
        let mut tick = interval(period);
        let mut tick_i = 0;
        self.cb.with_discovery(&self.discovery).await;
        let mut services = Services::new(&self.snapshot);

        loop {
            while let Ok((name, t)) = self.rx.try_recv() {
                services.register(name, t, &self.discovery).await;
            }
            let cycle_i = tick_i % cycle;
            for (idx, group) in services.groups.iter_mut().enumerate() {
                if idx % cycle == cycle_i {
                    group.refresh(&self.snapshot, &self.discovery).await;
                } else {
                    group.refresh_new(&self.snapshot, &self.discovery).await;
                }
                group.check_load();
            }

            // tick_i < cycle时，每个tick都check，增加扫描的效率。
            // tick_i >= cycle时，每个cycle的第一个tick才check，减少扫描的消耗。
            if !self.cb.inited() || cycle_i == 0 {
                self.cb.with_discovery(&self.discovery).await;
            }
            tick_i += 1;
            tick.tick().await;
        }
    }
}

struct Services<T> {
    snapshot: String,
    indices: HashMap<String, usize>,
    groups: Vec<ServiceGroup<T>>,
}
impl<T: TopologyWrite> Services<T> {
    fn new(snapshot: &str) -> Self {
        Self {
            snapshot: snapshot.to_string(),
            groups: Vec::with_capacity(64),
            indices: HashMap::with_capacity(64),
        }
    }
    fn get_group(&mut self, full_group: &str) -> Option<&mut ServiceGroup<T>> {
        let &idx = self.indices.get(full_group)?;
        assert!(idx < self.groups.len());
        Some(&mut self.groups[idx])
    }
    // name的格式。 dir0+dir1+dir2+...+group:namespace
    // 规范：最后一个+号之后的是group:namespace
    // namespace是可选。
    // namespace之前的是group的路径，如：分隔符为'+'。
    async fn register<D: Discover>(&mut self, name: String, top: T, d: &D) {
        log::info!("receive new service: {}", name);
        let group = name.split('+').last().expect("name");
        let mut group_namespace = group.split(':');
        let group = group_namespace.next().expect("group");
        let (full_group, service) = match group_namespace.next() {
            Some(ns) => (&name[..name.len() - ns.len() - 1], ns),
            None => (name.as_str(), group),
        };

        let g = match self.get_group(full_group) {
            Some(g) => g,
            None => {
                let idx = self.groups.len();
                let local = full_group.to_string();
                let remote = full_group.replace('+', "/");

                self.groups
                    .push(ServiceGroup::new(group.to_string(), local, remote));
                self.indices.insert(full_group.to_string(), idx);
                let g = self.groups.get_mut(idx).expect("not push");
                assert_eq!(g.name, group);
                g.init(&self.snapshot, d).await;
                g
            }
        };
        log::info!("register service: {} => {}", name, group);
        g.register(service.to_string(), top);
    }
}

// 一个group会有一个或者多个namespace,共享一个group配置。
struct ServiceGroup<T> {
    changed: bool,
    name: String, // group的名称。
    local_path: String,
    path: String, // 从discover获取配置时使用group的path。
    sig: String,
    cfg: String,
    cache: HashMap<String, String>,
    namespaces: Vec<Service<T>>,
}
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
impl<T: TopologyWrite> ServiceGroup<T> {
    // 先从snapshot获取配置。然后再从discover刷新。
    async fn init<D: Discover>(&mut self, snapshot: &str, d: &D) {
        self.load_from_snapshot(snapshot).await;
        self.load_from_discover(snapshot, d).await;
    }
    // load所有的namespace。
    // true: 表示load完成
    // false: 表示后续需要继续load
    fn check_load(&mut self) {
        for s in &mut self.namespaces {
            if s.top.need_load() {
                s.top.load();
            }
        }
    }
    // 第一行是sig
    // 第二行是group name
    // 后面是cfg
    async fn load_from_snapshot(&mut self, snapshot: &str) -> Option<()> {
        let path = format!("{}/{}", snapshot, self.local_path);
        // 返回第一行与剩余的内容
        fn take_line(mut s: String) -> Option<(String, String)> {
            let idx = s.find("\n")?;
            let left = s.split_off(idx + 1);
            s.pop();
            if s.len() > 0 && s.as_bytes()[s.len() - 1] == b'\r' {
                s.pop();
            }
            Some((s, left))
        }
        let content = tokio::fs::read_to_string(&path).await.ok()?;
        let (sig, group_cfg) = take_line(content)?;
        let (group, cfg) = take_line(group_cfg)?;

        if group != self.name {
            log::warn!("group name changed: {path} '{}' -> '{}'", self.name, group);
            return None;
        }
        log::info!("load from snapshot: {} {} => {}", path, self.sig, sig);
        self.sig = sig;
        self.cfg = cfg;
        self.changed = true;
        Some(())
    }
    // 第一行是sig
    // 第二行是group name
    // 后面是cfg
    async fn _dump_to_snapshot(&self, snapshot: &str) -> std::io::Result<()> {
        let path = format!("{}/{}", snapshot, self.local_path);
        log::info!("dump to snapshot: {}", path);
        let mut file = File::create(path).await?;
        file.write_all(&self.sig.as_bytes()).await?;
        file.write_all(b"\n").await?;
        file.write_all(&self.name.as_bytes()).await?;
        file.write_all(b"\n").await?;
        file.write_all(&self.cfg.as_bytes()).await?;
        Ok(())
    }
    async fn dump_to_snapshot(&self, snapshot: &str) {
        if let Err(e) = self._dump_to_snapshot(snapshot).await {
            log::warn!("failed to dump to snapshot: {}, {}", self.name, e);
        }
    }
    async fn load_from_discover<D: Discover>(&mut self, snapshot: &str, d: &D) {
        log::debug!("load from discover: {}", self.path);
        use crate::Config;
        match d.get_service::<String>(&self.path, &self.sig).await {
            Ok(Config::NotFound) => log::warn!("service not found: {}", self.path),
            Ok(Config::NotChanged) => log::debug!("service not changed: {}", self.path),
            Ok(Config::Config(sig, cfg)) => {
                log::info!("service changed: {} sig {} => {}", self.path, self.sig, sig);
                self.sig = sig;
                self.cfg = cfg;
                self.dump_to_snapshot(snapshot).await;
                self.cache.clear();
                self.changed = true;
            }
            Err(e) => log::error!("failed to get service: {}, {}", self.path, e),
        }
    }
    async fn refresh_new<D: Discover>(&mut self, snapshot: &str, d: &D) -> bool {
        if self.cfg.len() == 0 {
            self.load_from_discover(snapshot, d).await;
        }
        self.update_all(true)
    }
    async fn refresh<D: Discover>(&mut self, snapshot: &str, d: &D) -> bool {
        self.load_from_discover(snapshot, d).await;
        self.update_all(false)
    }
    // new: 只更新新注册的服务
    // 返回是否可能有namespace更新
    fn update_all(&mut self, new: bool) -> bool {
        if self.changed && self.namespaces.len() > 0 {
            let s = self.namespaces.first().expect("empty");
            self.cache = s
                .top
                .disgroup(&self.name, &self.cfg)
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            for s in self.namespaces.iter_mut() {
                // 如果是新注册的，则不更新
                if new && s.cfg.len() > 0 {
                    continue;
                }
                let ns = &s.name;
                match self.cache.get(ns) {
                    Some(cfg) => s.update(cfg),
                    None => log::warn!("namespace {} not found", ns),
                }
            }
            self.changed = false;
            return true;
        }
        false
    }
    fn register(&mut self, ns: String, top: T) {
        assert!(self.namespaces.iter().find(|s| s.name == ns).is_none());
        let service = Service::new(ns, top);
        self.namespaces.push(service);
        self.changed = true;
    }
    fn new(name: String, local_path: String, path: String) -> Self {
        Self {
            name,
            path,
            local_path,
            sig: String::new(),
            cfg: String::new(),
            namespaces: Vec::new(),
            cache: HashMap::new(),
            changed: false,
        }
    }
}

struct Service<T> {
    name: String,
    top: T,
    cfg: String,
    metric: Metric,
}
impl<T: TopologyWrite> Service<T> {
    fn new(name: String, top: T) -> Self {
        let metric = metrics::Path::new(vec!["any", name.as_str()]).num("top_updated");
        Self {
            name,
            top,
            cfg: String::new(),
            metric,
        }
    }
    fn update(&mut self, cfg: &str) {
        if cfg != self.cfg {
            self.cfg = cfg.to_string();
            self.top.update(&self.name, cfg);
            self.metric += 1;
            log::info!("Updated {}", self.name);
        }
    }
}
