use std::{
    collections::HashMap,
    net::IpAddr,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::sync::mpsc::{
    unbounded_channel, UnboundedReceiver as Receiver, UnboundedSender as Sender,
};

use trust_dns_resolver::{AsyncResolver, TokioConnection, TokioConnectionProvider, TokioHandle};

use ds::{CowReadHandle, CowWriteHandle};
#[ctor::ctor]
static DNSCACHE: CowReadHandle<DnsCache> = {
    let resolver = system_resolver();
    let (reg_tx, reg_rx) = unbounded_channel();
    let cache = DnsCache::from(reg_tx);
    let (tx, rx) = ds::cow(cache);
    let resolver = DnsResolver {
        tx,
        reg_rx,
        resolver,
    };
    *DNS_RESOLVER.try_lock().expect("lock failed") = Some(resolver);
    rx
};
use std::sync::Mutex;
static DNS_RESOLVER: Mutex<Option<DnsResolver>> = Mutex::new(None);

type RegisterItem = (String, Arc<AtomicBool>);
type Resolver = AsyncResolver<TokioConnection, TokioConnectionProvider>;

pub struct DnsResolver {
    tx: CowWriteHandle<DnsCache>,
    reg_rx: Receiver<RegisterItem>,
    resolver: Resolver,
}

pub fn register(host: &str) -> Arc<AtomicBool> {
    DNSCACHE.get().watch(host)
}

pub fn lookup_ips(host: &str) -> Vec<String> {
    // log::debug!("++++ lookup ips:{:?}", DNSCACHE.get().hosts);
    DNSCACHE.get().lookup(host)
}

fn system_resolver() -> Resolver {
    AsyncResolver::from_system_conf(TokioHandle).expect("crate dns resolver")
}

#[derive(Default, Clone, Debug)]
struct Record {
    stale: Arc<AtomicBool>,
    subscribers: Vec<Arc<AtomicBool>>,
    ips: Vec<IpAddr>,
    id: usize,
}
impl Record {
    fn first_watch(&self) -> Arc<AtomicBool> {
        assert_ne!(self.subscribers.len(), 0);
        self.subscribers[0].clone()
    }
    fn watch(&mut self, s: Arc<AtomicBool>) {
        self.subscribers.push(s);
    }
    fn update(&mut self, addr: Vec<IpAddr>) {
        self.ips = addr;
        self.stale.store(true, Ordering::Release);
    }
    fn stale(&self) -> bool {
        self.stale.load(Ordering::Acquire)
    }
    fn notify(&self) {
        if self.stale() {
            for update in self.subscribers.iter() {
                update.store(true, Ordering::Release);
            }
            self.stale.store(false, Ordering::Release);
        }
    }
    // 如果有更新，则返回lookup的ip。
    // 无更新则返回None
    async fn check_refresh(&self, host: &str, resolver: &mut Resolver) -> Option<Vec<IpAddr>> {
        match resolver.lookup_ip(host).await {
            Ok(ips) => {
                // 必须有IP。
                let mut exists = 0;
                let mut cnt = 0;
                for ip in ips.iter() {
                    log::debug!("{} resolved ip {}", host, ip);
                    cnt += 1;
                    if self.ips.contains(&ip) {
                        exists += 1;
                    }
                }
                if cnt == 0 {
                    log::info!("no ip resolved: {}", host);
                    return None;
                }
                if exists != self.ips.len() || exists != cnt {
                    let mut addrs = Vec::with_capacity(cnt);
                    for ip in ips.iter() {
                        addrs.push(ip);
                    }
                    if self.ips.len() > 0 {
                        log::info!("host {} refreshed from {:?} to {:?}", host, self.ips, addrs);
                    }
                    return Some(addrs);
                }
            }
            Err(_e) => {
                log::info!("refresh host failed:{}, {:?}", host, _e);
            }
        }
        None
    }
    async fn refresh(&mut self, host: &str, resolver: &mut Resolver) {
        if let Some(addrs) = self.check_refresh(host, resolver).await {
            self.ips = addrs;
        }
    }
}

pub trait IPPort {
    fn host(&self) -> &str;
    fn port(&self) -> &str;
}

impl IPPort for &str {
    #[inline]
    fn host(&self) -> &str {
        let idx = self.find(":").unwrap_or(self.len());
        &self[..idx]
    }
    #[inline]
    fn port(&self) -> &str {
        let idx = self.find(":").map(|idx| idx + 1).unwrap_or(self.len());
        &self[idx..]
    }
}
impl IPPort for String {
    #[inline]
    fn host(&self) -> &str {
        let idx = self.find(":").unwrap_or(self.len());
        &self[..idx]
    }
    #[inline]
    fn port(&self) -> &str {
        let idx = self.find(":").map(|idx| idx + 1).unwrap_or(self.len());
        &self[idx..]
    }
}

pub async fn start_dns_resolver_refresher() {
    log::info!("task started ==> dns cache refresher");
    let dns_resolver = DNS_RESOLVER
        .try_lock()
        .expect("lock failed")
        .take()
        .expect("not inited");
    let mut cache = dns_resolver.tx;
    let mut rx = dns_resolver.reg_rx;
    let mut resolver = dns_resolver.resolver;
    use ds::time::{Duration, Instant};
    const BATCH_CNT: usize = 128;
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    //let mut last = Instant::now(); // 上一次刷新的时间
    let mut idx = 0;
    loop {
        let mut regs = Vec::new();
        while let Ok(reg) = rx.try_recv() {
            regs.push(reg);
        }
        if regs.len() > 0 {
            let mut w_cache = cache.get().clone();
            for reg in regs {
                w_cache.register(reg.0.clone(), reg.1.clone());
                w_cache.refresh_one(&reg.0, &mut resolver).await;
            }
            cache.update(w_cache);
            cache.get().notify();
            // 再次快速尝试读取新注册的数据
            continue;
        }

        // 每一秒种tick一次，检查是否
        tick.tick().await;

        let _start = Instant::now();
        let mut updated = HashMap::new();
        let r_cache = cache.get();

        for (host, record) in &r_cache.hosts {
            if idx == record.id % BATCH_CNT {
                if let Some(addrs) = record.check_refresh(host, &mut resolver).await {
                    updated.insert(host.to_string(), addrs);
                }
            }
        }

        idx = (idx + 1) % BATCH_CNT;
        drop(r_cache);
        if updated.len() > 0 {
            let mut new = cache.get().clone();
            for (host, addrs) in updated.into_iter() {
                new.hosts
                    .get_mut(&host)
                    .expect("insert before")
                    .update(addrs);
            }
            cache.update(new);
            cache.get().notify();
        }
        //last = Instant::now();

        log::trace!("refresh dns elapsed:{:?}", _start.elapsed());
    }
}

#[derive(Clone)]
pub struct DnsCache {
    tx: Sender<RegisterItem>,
    hosts: HashMap<String, Record>,
}
impl DnsCache {
    fn from(tx: Sender<RegisterItem>) -> Self {
        Self {
            tx,
            hosts: Default::default(),
        }
    }
    fn notify(&self) {
        for (_host, record) in &self.hosts {
            if record.stale() {
                record.notify();
                log::debug!("host {} refreshed and notified {:?}", _host, record.ips);
            }
        }
    }
    fn watch(&self, addr: &str) -> Arc<AtomicBool> {
        match self.hosts.get(addr) {
            Some(record) => record.first_watch(),
            None => {
                log::debug!("{} watching", addr);
                let watcher = Arc::new(AtomicBool::new(false));
                if let Err(_e) = self.tx.send((addr.to_string(), watcher.clone())) {
                    log::info!("watcher failed to {} => {:?}", addr, _e);
                }
                watcher
            }
        }
    }
    fn register(&mut self, host: String, notify: Arc<AtomicBool>) {
        log::debug!("host {} registered to cache", host);
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let id = SEQ.fetch_add(1, Ordering::Relaxed);
        let r = self.hosts.entry(host).or_default();
        r.id = id;
        r.watch(notify);
    }
    fn lookup(&self, host: &str) -> Vec<String> {
        let mut addrs = Vec::new();
        if let Some(record) = self.hosts.get(host) {
            addrs.reserve(record.ips.len());
            for addr in record.ips.iter() {
                match addr {
                    IpAddr::V4(ip) => addrs.push(ip.to_string()),
                    _ => {}
                }
            }
        }
        addrs
    }
    async fn refresh_one(&mut self, host: &str, resolver: &mut Resolver) {
        self.hosts
            .get_mut(host)
            .expect("not register")
            .refresh(host, resolver)
            .await;
    }
}
