use once_cell::sync::OnceCell;
use std::{
    collections::HashMap,
    future::Future,
    net::Ipv4Addr as IpAddr,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender as Sender};

mod lookup;
use lookup::Lookup;

use ds::{
    time::{interval, Duration, Instant},
    CowReadHandle, ReadGuard,
};
static DNSCACHE: OnceCell<CowReadHandle<DnsCache>> = OnceCell::new();

type RegisterItem = (String, Arc<AtomicBool>);

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
    subscribers: Vec<Arc<AtomicBool>>,
    ips: Vec<IpAddr>,
    id: usize,
    md5: u64,     // 使用所有ip的和作为md5
    notify: bool, // true表示需要通知
}
impl Record {
    fn watch(&mut self, s: Arc<AtomicBool>) {
        self.subscribers.push(s);
    }
    fn need_notify(&self) -> bool {
        self.notify
    }
    fn notify(&mut self) {
        assert!(self.notify);
        for update in self.subscribers.iter() {
            update.store(true, Ordering::Release);
        }
        self.notify = false;
    }
    fn empty(&self) -> bool {
        self.ips.len() == 0
    }
    // 如果有更新，则返回lookup的ip。
    // 无更新则返回None
    async fn refresh(&mut self, host: &str, r: &mut Lookup) -> bool {
        match r.lookup(host).await {
            Ok(ips) => {
                let mut md5 = 0u64;
                let mut cnt = 0;
                for ip in ips.iter() {
                    match ip {
                        std::net::IpAddr::V4(ip) => {
                            md5 += u32::from(*ip) as u64;
                            cnt += 1;
                        }
                        std::net::IpAddr::V6(_ip) => {}
                    }
                }
                log::debug!("{} resolved ips:{:?}, md5:{}", host, ips, md5);
                if cnt > 0 && (cnt != self.ips.len() || self.md5 != md5) {
                    self.ips = ips
                        .into_iter()
                        .map(|ip| match ip {
                            std::net::IpAddr::V4(ip) => ip,
                            _ => unreachable!(),
                        })
                        .collect();
                    self.md5 = md5;
                    self.notify = true;
                    return true;
                }
            }
            Err(e) => log::info!("refresh host failed:{}, {:?}", host, e),
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
    let (reg_tx, mut reg_rx) = unbounded_channel();
    let mut local_cache = DnsCache::from(reg_tx);
    let (mut writer, reader) = ds::cow(local_cache.clone());
    let _r = DNSCACHE.set(reader);
    assert!(_r.is_ok(), "dns cache set failed");
    async move {
        let mut resolver = Lookup::new();
        log::info!("task started ==> dns cache refresher");
        let mut tick = interval(Duration::from_secs(1));
        let mut idx = 0;
        loop {
            while let Ok((host, notify)) = reg_rx.try_recv() {
                local_cache.register(host, notify);
            }
            let start = Instant::now();
            let (num, cache) = local_cache.refresh_by_idx(idx, &mut resolver).await;
            if num > 0 {
                let new = local_cache.clone();
                writer.update(new);
                local_cache.notify(cache.expect("cache none"), num);
            }

            idx += 1;
            log::trace!("refresh dns elapsed:{:?}", start.elapsed());
            tick.tick().await;
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
            hosts: HashMap::with_capacity(4096),
        }
    }
    fn watch(&self, addr: &str, notify: Arc<AtomicBool>) {
        log::debug!("{} watching", addr);
        if let Err(_e) = self.tx.send((addr.to_string(), notify)) {
            log::error!("watcher failed to {} => {:?}", addr, _e);
        }
    }
    // 刷新所有id是idx的record。如果record的ips长度为空，也一共刷新。
    // 1. 返回刷新成功的数量；
    // 2. 如果刷新成功的数量大于1，则返回第一个刷新的addr。
    // 通常不会有同时多个record刷新，可以使用这个减少flush_to时的遍历。
    async fn refresh_by_idx(
        &mut self,
        idx: usize,
        resolver: &mut Lookup,
    ) -> (usize, Option<String>) {
        let mut refreshes = 0usize;
        let mut cache = None;
        const BATCH_CNT: usize = 128;
        let idx = idx % BATCH_CNT;
        for (host, record) in &mut self.hosts {
            // 新注册的host要及时刷新，从来没有解析成功的也一直要刷新
            if idx == record.id % BATCH_CNT || record.empty() {
                if record.refresh(host, resolver).await {
                    (refreshes == 0).then(|| cache.insert(host.clone()));
                    refreshes += 1;
                }
            }
        }
        (refreshes, cache)
    }
    fn notify(&mut self, cache: String, refreshes: usize) {
        assert!(refreshes >= 1);
        // 先更新，再通知。
        // 通知
        if refreshes == 1 {
            // shrink主要是减少cap，以提升遍历性能
            self.hosts.shrink_to_fit();
            let record = self.hosts.get_mut(&cache).unwrap();
            record.notify();
        } else {
            // 遍历，notify为true的需要通知
            let mut n = 0;
            for (_, record) in self.hosts.iter_mut() {
                if record.need_notify() {
                    record.notify();
                    n += 1;
                }
            }
            assert_eq!(n, refreshes);
        }
    }
    fn register(&mut self, host: String, notify: Arc<AtomicBool>) {
        log::debug!("host {} registered to cache", host);
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let id = SEQ.fetch_add(1, Ordering::Relaxed);
        let r = self.hosts.entry(host.clone()).or_default();
        r.id = id;
        r.watch(notify);
    }
    fn lookup(&self, host: &str) -> &[IpAddr] {
        static EMPTY: Vec<IpAddr> = Vec::new();
        self.hosts
            .get(host)
            .map(|r| &r.ips)
            .unwrap_or_else(|| &EMPTY)
    }
}
