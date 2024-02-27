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
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender as Sender};

use trust_dns_resolver::TokioAsyncResolver;

use ds::{
    time::{interval, Duration, Instant},
    CowReadHandle, ReadGuard,
};
static DNSCACHE: OnceCell<CowReadHandle<DnsCache>> = OnceCell::new();

type RegisterItem = (String, Arc<AtomicBool>);
type Resolver = TokioAsyncResolver;

fn get_dns() -> ReadGuard<DnsCache> {
    //必须使用get进行线程间同步
    DNSCACHE.get().expect("handler not started").get()
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
        (self.md5 != 0).then(|| log::info!("record changed: {self:?} => {addr_md5:?}"));
        debug_assert_ne!(self.ips, addr_md5.0);
        debug_assert_ne!(self.md5, addr_md5.1);
        (self.ips, self.md5) = addr_md5;
    }
    fn notify(&self) {
        for update in self.subscribers.iter() {
            update.store(true, Ordering::Release);
        }
    }
    // 如果有更新，则返回lookup的ip。
    // 无更新则返回None
    async fn check_refresh(&self, r: &mut Resolver) -> Option<(Vec<IpAddr>, u64)> {
        let host = &self.host;
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
            Err(e) => log::info!("refresh host failed:{}, {:?}", host, e),
        }
        None
    }
    async fn refresh(&mut self, resolver: &mut Resolver) -> bool {
        if let Some((addrs, md5)) = self.check_refresh(resolver).await {
            self.update((addrs, md5));
            return true;
        }
        false
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
    let (tx, rx) = ds::cow(DnsCache::from(reg_tx));
    let _r = DNSCACHE.set(rx);
    assert!(_r.is_ok(), "dns cache set failed");
    async move {
        let mut resolver = TokioAsyncResolver::tokio_from_system_conf().expect("resolver");
        log::info!("task started ==> dns cache refresher");
        let mut cache = tx;
        let mut rx = reg_rx;
        const BATCH_CNT: usize = 128;
        let mut tick = interval(Duration::from_secs(1));
        let mut idx = 0;
        let mut w_cache = None;
        let mut need_notify = Vec::new();
        loop {
            if let Ok(reg) = rx.try_recv() {
                let w = w_cache.get_or_insert_with(|| cache.copy());
                let r = w.register(reg.0, reg.1);
                r.refresh(&mut resolver)
                    .await
                    .then(|| need_notify.push(r.host.clone()));
                continue;
            }
            // 第一次增量更新，不等待tick
            let notify = |cache: &mut ds::CowWriteHandle<DnsCache>,
                          w_cache: Option<DnsCache>,
                          need_notify: &mut Vec<String>| {
                if let Some(w) = w_cache {
                    cache.update(w);
                    let hosts = &cache.get().hosts;
                    for host in need_notify.iter() {
                        hosts.get(host).map(|r| r.notify());
                    }
                    need_notify.clear();
                }
            };
            notify(&mut cache, w_cache.take(), &mut need_notify);
            // 每一秒种tick一次，检查是否
            tick.tick().await;
            let start = Instant::now();
            for (host, record) in &cache.get().hosts {
                assert_eq!(host, &record.host);
                if idx == record.id % BATCH_CNT {
                    if let Some(addrs) = record.check_refresh(&mut resolver).await {
                        let w = w_cache.get_or_insert_with(|| cache.copy());
                        w.hosts.get_mut(host).expect("insert before").update(addrs);
                        need_notify.push(host.clone());
                    }
                }
            }
            // 第二次增量更新，每个tick只更新一部分(1/BATCH_CNT)
            notify(&mut cache, w_cache.take(), &mut need_notify);
            need_notify.shrink_to(1);

            idx = (idx + 1) % BATCH_CNT;
            log::trace!("refresh dns elapsed:{:?}", start.elapsed());
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
    fn register(&mut self, host: String, notify: Arc<AtomicBool>) -> &mut Record {
        log::debug!("host {} registered to cache", host);
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let id = SEQ.fetch_add(1, Ordering::Relaxed);
        let r = self.hosts.entry(host.clone()).or_default();
        r.id = id;
        r.host = host;
        r.watch(notify);
        r
    }
    fn lookup(&self, host: &str) -> &[IpAddr] {
        static EMPTY: Vec<IpAddr> = Vec::new();
        self.hosts
            .get(host)
            .map(|r| &r.ips)
            .unwrap_or_else(|| &EMPTY)
    }
}
