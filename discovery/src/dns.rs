use once_cell::sync::OnceCell;
use std::{
    collections::HashMap,
    future::Future,
    net::IpAddr,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::sync::mpsc::{
    unbounded_channel, UnboundedReceiver as Receiver, UnboundedSender as Sender,
};

use trust_dns_resolver::TokioAsyncResolver;

use ds::{time::interval, CowReadHandle, CowWriteHandle, ReadGuard};
static DNSCACHE: OnceCell<CowReadHandle<DnsCache>> = OnceCell::new();

type RegisterItem = (String, Arc<AtomicBool>);
type Resolver = TokioAsyncResolver;

pub struct DnsResolver {
    tx: CowWriteHandle<DnsCache>,
    reg_rx: Receiver<RegisterItem>,
    resolver: Resolver,
}

fn get_dns() -> ReadGuard<DnsCache> {
    //必须使用get进行线程间同步
    DNSCACHE.get().unwrap().get()
}

pub fn register(host: &str, notify: Arc<AtomicBool>) {
    get_dns().watch(host, notify)
}
pub fn lookup_ips<'a>(host: &str, mut f: impl FnMut(&[IpAddr])) {
    f(get_dns().lookup(host))
}

#[derive(Default, Clone, Debug)]
struct Record {
    host: String,
    subscribers: Vec<Arc<AtomicBool>>,
    ips: Vec<IpAddr>,
    id: usize,
    md5: u64, // 使用所有ip的和作为md5
}
impl Record {
    fn watch(&mut self, s: Arc<AtomicBool>) {
        self.subscribers.push(s);
    }
    fn update(&mut self, addr_md5: (Vec<IpAddr>, u64)) {
        if self.md5 != 0 {
            log::info!("update dns record: {:?} => {:?}", self, addr_md5);
        }
        debug_assert_ne!(self.ips, addr_md5.0);
        debug_assert_ne!(self.md5, addr_md5.1);
        self.ips = addr_md5.0;
        self.md5 = addr_md5.1;
        self.notify();
    }
    fn notify(&self) {
        for update in self.subscribers.iter() {
            update.store(true, Ordering::Release);
        }
    }
    // 如果有更新，则返回lookup的ip。
    // 无更新则返回None
    async fn check_refresh(&self, host: &str, r: &mut Resolver) -> Option<(Vec<IpAddr>, u64)> {
        match r.lookup_ip(host).await {
            Ok(ips) => {
                let mut md5 = 0u64;
                let mut cnt = 0;
                for ip in ips.iter() {
                    log::debug!("{} resolved ip {}", host, ip);
                    if let IpAddr::V4(v4) = ip {
                        cnt += 1;
                        let bits: u32 = v4.into();
                        md5 += bits as u64;
                    }
                }
                log::debug!("{} resolved ips:{:?}, md5:{}", host, ips, md5);
                if cnt > 0 && (cnt != self.ips.len() || self.md5 != md5) {
                    let mut addrs = Vec::with_capacity(cnt);
                    for ip in ips.iter() {
                        addrs.push(ip);
                    }
                    return Some((addrs, md5));
                }
            }
            Err(_e) => {
                log::info!("refresh host failed:{}, {:?}", host, _e);
            }
        }
        None
    }
    async fn refresh(&mut self, host: &str, resolver: &mut Resolver) {
        if let Some((addrs, md5)) = self.check_refresh(host, resolver).await {
            self.update((addrs, md5));
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

pub fn start_dns_resolver_refresher() -> impl Future<Output = ()> {
    let (reg_tx, reg_rx) = unbounded_channel();
    let cache = DnsCache::from(reg_tx);
    let (tx, rx) = ds::cow(cache);
    let _ = DNSCACHE.set(rx);
    async move {
        let resolver = TokioAsyncResolver::tokio_from_system_conf().expect("crate dns resolver");
        let resolver = DnsResolver {
            tx,
            reg_rx,
            resolver,
        };
        log::info!("task started ==> dns cache refresher");
        let mut cache = resolver.tx;
        let mut rx = resolver.reg_rx;
        let mut resolver = resolver.resolver;
        use ds::time::{Duration, Instant};
        const BATCH_CNT: usize = 128;
        let mut tick = interval(Duration::from_secs(1));
        //let mut last = Instant::now(); // 上一次刷新的时间
        let mut idx = 0;
        loop {
            let mut regs = Vec::new();
            while let Ok(reg) = rx.try_recv() {
                if regs.capacity() == 0 {
                    regs.reserve(16);
                }
                regs.push(reg);
            }
            if regs.len() > 0 {
                let mut w_cache = cache.copy();
                for reg in regs {
                    w_cache.register(reg.0.clone(), reg.1.clone());
                    w_cache.refresh_one(&reg.0, &mut resolver).await;
                }
                cache.update(w_cache);
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
                cache.write(|c| {
                    for (host, addrs) in updated.into_iter() {
                        c.hosts.get_mut(&host).expect("insert before").update(addrs);
                    }
                });
            }
            //last = Instant::now();

            log::trace!("refresh dns elapsed:{:?}", _start.elapsed());
        }
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
    fn watch(&self, addr: &str, notify: Arc<AtomicBool>) {
        log::debug!("{} watching", addr);
        if let Err(_e) = self.tx.send((addr.to_string(), notify)) {
            log::error!("watcher failed to {} => {:?}", addr, _e);
        }
    }
    fn register(&mut self, host: String, notify: Arc<AtomicBool>) {
        log::debug!("host {} registered to cache", host);
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let id = SEQ.fetch_add(1, Ordering::Relaxed);
        let r = self.hosts.entry(host.clone()).or_default();
        r.id = id;
        r.host = host;
        r.watch(notify);
    }
    fn lookup(&self, host: &str) -> &[IpAddr] {
        static EMPTY: Vec<IpAddr> = Vec::new();
        self.hosts
            .get(host)
            .map(|r| &r.ips)
            .unwrap_or_else(|| &EMPTY)
    }
    async fn refresh_one(&mut self, host: &str, resolver: &mut Resolver) {
        self.hosts
            .get_mut(host)
            .expect("not register")
            .refresh(host, resolver)
            .await;
    }
}
