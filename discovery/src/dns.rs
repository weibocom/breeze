use std::{
    collections::HashMap,
    net::IpAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::sync::mpsc::{
    unbounded_channel, UnboundedReceiver as Receiver, UnboundedSender as Sender,
};

use trust_dns_resolver::{AsyncResolver, TokioConnection, TokioConnectionProvider, TokioHandle};

use ds::{CowReadHandle, CowWriteHandle};
use once_cell::sync::OnceCell;
static DNSCACHE: OnceCell<CowReadHandle<DnsCache>> = OnceCell::new();

type RegisterItem = (String, Arc<AtomicBool>);
type Resolver = AsyncResolver<TokioConnection, TokioConnectionProvider>;

pub async fn start_dns_resolver_refresher() {
    let resolver = system_resolver();
    let (reg_tx, reg_rx) = unbounded_channel();
    let cache = DnsCache::from(reg_tx);
    let (tx, rx) = ds::cow(cache);
    let _ = DNSCACHE.set(rx);

    start_watch_dns(tx, reg_rx, resolver).await
}

pub fn register(host: &str) -> Arc<AtomicBool> {
    DNSCACHE.get().expect("not inited").get().watch(host)
}

pub fn lookup_ips(host: &str) -> Vec<String> {
    DNSCACHE
        .get()
        .expect("not inited dnscache")
        .get()
        .lookup(host)
}

fn system_resolver() -> Resolver {
    AsyncResolver::from_system_conf(TokioHandle).expect("crate dns resolver")
}

#[derive(Default, Clone, Debug)]
struct Record {
    stale: Arc<AtomicBool>,
    subscribers: Vec<Arc<AtomicBool>>,
    ips: Vec<IpAddr>,
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
    // ???????????????????????????lookup???ip???
    // ??????????????????None
    async fn check_refresh(&self, host: &str, resolver: &mut Resolver) -> Option<Vec<IpAddr>> {
        match resolver.lookup_ip(host).await {
            Ok(ips) => {
                // ?????????IP???
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
            Err(e) => {
                log::info!("refresh host failed:{}, {:?}", host, e);
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

async fn start_watch_dns(
    mut cache: CowWriteHandle<DnsCache>,
    mut rx: Receiver<RegisterItem>,
    mut resolver: Resolver,
) {
    log::info!("task started ==> dns cache refresher");
    use std::task::Poll;
    let noop = noop_waker::noop_waker();
    let mut ctx = std::task::Context::from_waker(&noop);
    use std::time::{Duration, Instant};
    const CYCLE: Duration = Duration::from_secs(79);
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let mut last = Instant::now(); // ????????????????????????
    loop {
        let mut regs = Vec::new();
        while let Poll::Ready(Some(reg)) = rx.poll_recv(&mut ctx) {
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
            // ??????????????????????????????????????????
            continue;
        }
        // ????????????tick?????????????????????
        tick.tick().await;
        if last.elapsed() < CYCLE {
            continue;
        }
        let start = Instant::now();
        let mut updated = HashMap::new();
        let r_cache = cache.get();
        for (host, record) in &r_cache.hosts {
            if let Some(addrs) = record.check_refresh(host, &mut resolver).await {
                updated.insert(host.to_string(), addrs);
            }
        }
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
        last = Instant::now();

        log::debug!("refresh dns elapsed:{:?}", start.elapsed());
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
        for (host, record) in &self.hosts {
            if record.stale() {
                record.notify();
                log::debug!("host {} refreshed and notified {:?}", host, record.ips);
            }
        }
    }
    fn watch(&self, addr: &str) -> Arc<AtomicBool> {
        match self.hosts.get(addr) {
            Some(record) => record.first_watch(),
            None => {
                log::debug!("{} watching", addr);
                let watcher = Arc::new(AtomicBool::new(false));
                if let Err(e) = self.tx.send((addr.to_string(), watcher.clone())) {
                    log::info!("watcher failed to {} => {:?}", addr, e);
                }
                watcher
            }
        }
    }
    fn register(&mut self, host: String, notify: Arc<AtomicBool>) {
        log::debug!("host {} registered to cache", host);
        self.hosts.entry(host).or_default().watch(notify);
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
