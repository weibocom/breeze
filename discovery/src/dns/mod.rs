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
use lookup::*;

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

#[derive(Clone, Debug)]
struct Record {
    subscribers: Vec<Arc<AtomicBool>>,
    ips: Vec<IpAddr>,
    id: usize,
    md5: u64,     // 使用所有ip的和作为md5
    notify: bool, // true表示需要通知
}
impl Default for Record {
    fn default() -> Self {
        Record {
            subscribers: Vec::with_capacity(2),
            ips: Vec::with_capacity(2),
            id: 0,
            md5: 0,
            notify: false,
        }
    }
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
    fn refresh(&mut self, host: &str, ips: IpAddrLookup) -> bool {
        let mut md5 = 0u64;
        let mut cnt = 0;
        ips.visit_v4(|ip| {
            cnt += 1;
            md5 += u32::from(ip) as u64;
        });
        if cnt > 0 && (cnt != self.ips.len() || self.md5 != md5) {
            self.ips.clear();
            ips.visit_v4(|ip| self.ips.push(ip));
            self.md5 = md5;
            self.notify = true;
            log::debug!("{} resolved ips:{:?}, md5:{}", host, self.ips, md5);
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
    let (reg_tx, mut reg_rx) = unbounded_channel();
    let mut local_cache = DnsCache::from(reg_tx);
    let (mut writer, reader) = ds::cow(local_cache.clone());
    let _r = DNSCACHE.set(reader);
    assert!(_r.is_ok(), "dns cache set failed");
    async move {
        let resolver = Lookup::new();
        log::info!("task started ==> dns cache refresher");
        let mut tick = interval(Duration::from_secs(1));
        let mut idx = 0;
        loop {
            while let Ok((host, notify)) = reg_rx.try_recv() {
                local_cache.register(host, notify);
            }
            let start = Instant::now();
            let (num, cache) = resolver.lookups(local_cache.iter(idx)).await;
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
    fn iter(&mut self, idx: usize) -> HostRecordIter<'_> {
        const PERIOD: usize = 128;
        HostRecordIter::new(self, idx, PERIOD)
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

struct HostRecordIter<'a> {
    // 当前遍历的hist的id的索引
    idx: usize,
    iter: std::collections::hash_map::IterMut<'a, String, Record>,
    period: usize,
}

impl<'a> HostRecordIter<'a> {
    fn new(cache: &'a mut DnsCache, idx: usize, period: usize) -> Self {
        let idx = idx % period;
        let iter = cache.hosts.iter_mut();
        Self { idx, iter, period }
    }
}
// 实现Iterator trait
impl<'a> Iterator for HostRecordIter<'a> {
    type Item = (&'a str, &'a mut Record);
    fn next(&mut self) -> Option<Self::Item> {
        while let Some((host, record)) = self.iter.next() {
            if record.id % self.period == self.idx || record.empty() {
                return Some((host, record));
            }
        }
        None
    }
}
