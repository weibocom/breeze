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
static REGISTER: OnceCell<Sender<RegisterItem>> = OnceCell::new();

type RegisterItem = (String, Arc<AtomicBool>);
type Resolver = AsyncResolver<TokioConnection, TokioConnectionProvider>;

pub async fn start_dns_resolver_refresher() {
    let resolver = system_resolver();
    let (tx, rx) = ds::cow(Default::default());
    let (reg_tx, reg_rx) = unbounded_channel();
    let _ = DNSCACHE.set(rx);
    let _ = REGISTER.set(reg_tx);

    start_watch_dns(tx, reg_rx, resolver).await
}

pub fn register(host: &str, notify: Arc<AtomicBool>) {
    if let Err(e) = REGISTER
        .get()
        .expect("not inited")
        .send((host.to_string(), notify))
    {
        log::warn!("register host watcher failed. {}:{:?}", host, e);
    }
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
    stale: bool,
    subscribers: Vec<Arc<AtomicBool>>,
    ips: Vec<IpAddr>,
}
impl Record {
    fn watch(&mut self, s: Arc<AtomicBool>) {
        self.subscribers.push(s);
    }
    fn update(&mut self, addr: &Vec<IpAddr>) {
        self.ips = addr.clone();
        self.stale;
    }
    fn notify(&self) {
        if self.stale {
            for update in self.subscribers.iter() {
                update.store(true, Ordering::Release);
            }
        }
    }
    // 如果有更新，则返回等更新的ip。
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
                if self.ips.len() == 0 || exists != self.ips.len() {
                    let mut addrs = Vec::new();
                    for ip in ips.iter() {
                        addrs.push(ip);
                    }
                    return Some(addrs);
                }
            }
            Err(e) => {
                log::debug!("refresh host failed:{}, {:?}", host, e);
            }
        }
        None
    }
    async fn refresh(&mut self, host: &str, resolver: &mut Resolver) {
        if let Some(addrs) = self.check_refresh(host, resolver).await {
            // 第一次注册不输出日志。变更时再输出
            if self.ips.len() > 0 {
                log::info!("host {} updated from {:?} to {:?}", host, self.ips, addrs);
            }
            self.ips = addrs;
            self.stale = true;
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
    const CYCLE: Duration = Duration::from_secs(57);
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let mut last = Instant::now(); // 上一次刷新的时间
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
            // 更新cache
            cache.write(move |t| *t = w_cache.clone());
            cache.get().notify();
        }
        // 每一秒种tick一次，检查是否
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
            cache.write(|t| {
                for (host, addrs) in updated.iter() {
                    t.hosts.get_mut(host).expect("insert before").update(addrs);
                }
            });
            cache.get().notify();
        }
        last = Instant::now();

        log::info!("refresh dns elapsed:{:?}", start.elapsed());
    }
}

#[derive(Clone, Default)]
pub struct DnsCache {
    hosts: HashMap<String, Record>,
}
impl DnsCache {
    fn notify(&self) {
        for (host, record) in &self.hosts {
            if record.stale {
                record.notify();
                log::debug!("host {} refreshed {:?}", host, record.ips);
            }
        }
    }
    fn register(&mut self, host: String, notify: Arc<AtomicBool>) {
        log::debug!("host {} registered to cache", host);
        let record = self.hosts.entry(host).or_default();
        record.watch(notify);
    }
    fn lookup(&self, host: &str) -> Vec<String> {
        let mut addrs = Vec::new();
        if let Some(record) = self.hosts.get(host) {
            for addr in record.ips.iter() {
                match addr {
                    IpAddr::V4(ip) => addrs.push(ip.to_string()),
                    _ => {}
                }
            }
        }
        addrs
    }
    pub async fn refresh_one(&mut self, host: &str, resolver: &mut Resolver) {
        if let Some(record) = self.hosts.get_mut(host) {
            record.refresh(host, resolver).await;
        }
    }
    pub async fn refresh(&mut self, resolver: &mut Resolver) {
        for (host, record) in self.hosts.iter_mut() {
            record.refresh(host, resolver).await;
        }
    }
}
