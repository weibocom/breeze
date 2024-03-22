use once_cell::sync::OnceCell;
use std::{
    collections::HashMap,
    future::Future,
    net::Ipv4Addr as IpAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender as Sender};

mod lookup;
use lookup::*;

use ds::{
    time::{interval, Duration},
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
    md5: u64,     // 使用所有ip的和作为md5
    notify: bool, // true表示需要通知
}
impl Default for Record {
    fn default() -> Self {
        Record {
            subscribers: Vec::with_capacity(2),
            ips: Vec::with_capacity(2),
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
        let mut resolver = Lookup::new();
        log::info!("task started ==> dns cache refresher");
        let mut tick = interval(Duration::from_secs(1));
        loop {
            while let Ok((host, notify)) = reg_rx.try_recv() {
                local_cache.register(host, notify);
            }
            // 处理新增的
            (writer, resolver, local_cache) = tokio::task::spawn_blocking(move || {
                let (num, cache) = resolver.lookups(local_cache.iter());
                if num > 0 {
                    let new = local_cache.clone();
                    writer.update(new);
                    local_cache.notify(cache.expect("cache none"), num);
                }
                (writer, resolver, local_cache)
            })
            .await
            .expect("no err");

            tick.tick().await;
        }
    }
}

#[derive(Clone)]
pub struct DnsCache {
    tx: Sender<RegisterItem>,
    hosts: Hosts,
    last_idx: usize, // 刷新到的idx
    last_len: usize, // 上次刷新的长度
    cycle: usize,
}
impl DnsCache {
    fn from(tx: Sender<RegisterItem>) -> Self {
        Self {
            tx,
            hosts: Default::default(),
            last_idx: 0,
            last_len: 0,
            cycle: 0,
        }
    }
    fn watch(&self, addr: &str, notify: Arc<AtomicBool>) {
        log::debug!("{} watching", addr);
        if let Err(_e) = self.tx.send((addr.to_string(), notify)) {
            log::error!("watcher failed to {} => {:?}", addr, _e);
        }
    }
    // 每16个tick执行一次empty，避免某一次刷新未解释成功导致需要等待下一个周期。
    // 其他情况下，每个tick只会刷新部分chunk数据。
    fn iter(&mut self) -> HostRecordIter<'_> {
        const PERIOD: usize = 128;
        let ith = self.cycle % PERIOD;
        // 如果当前是走空搜索，下一次扫描的时候会搜索2个chunk.
        self.cycle += 1;
        // 16是一个经验值。
        if self.hosts.len() > self.last_len || (self.cycle % 16 == 0 && self.hosts.has_empty()) {
            self.last_len = self.hosts.len();
            HostRecordIter::empty_iter(self.hosts.hosts.iter_mut())
        } else {
            // 刷新从上个idx开始的一个chunk长度的数据
            assert!(self.last_len == self.hosts.len());
            let len = self.last_len;
            let chunk = (len + (PERIOD - 1)) / PERIOD;
            // 因为chunk是动态变化的，所以不能用last_idx + chunk
            let end = ((ith + 1) * chunk).min(len);
            assert!(self.last_idx <= end);
            let iter = self.hosts.hosts[self.last_idx..end].iter_mut();
            self.last_idx = end;
            if self.last_idx == len {
                self.cycle = 0;
                self.last_idx = 0;
            }
            HostRecordIter::new(iter)
        }
    }
    fn notify(&mut self, cache: String, refreshes: usize) {
        assert!(refreshes >= 1);
        // 先更新，再通知。
        // 通知
        if refreshes == 1 {
            let record = self.hosts.get_mut(&cache);
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
        let r = self.hosts.get_or_insert(host);
        r.watch(notify);
    }
    fn lookup(&self, host: &str) -> &[IpAddr] {
        static EMPTY: Vec<IpAddr> = Vec::new();
        self.hosts.get(host).unwrap_or_else(|| &EMPTY)
    }
}

#[derive(Clone)]
struct Hosts {
    // 只有注册与lookup的时候才需要使用cache进行判断是否已经存在, 这个只在变更的时候使用。
    index: HashMap<String, usize>,
    hosts: Vec<(String, Record)>,
}
impl Default for Hosts {
    fn default() -> Self {
        const CAP: usize = 2048;
        Self {
            index: HashMap::with_capacity(CAP),
            hosts: Vec::with_capacity(CAP),
        }
    }
}
impl Hosts {
    fn iter_mut(&mut self) -> std::slice::IterMut<'_, (String, Record)> {
        self.hosts.iter_mut()
    }
    fn get_mut(&mut self, host: &String) -> &mut Record {
        let &id = self.index.get(host).expect("not register");
        assert!(id < self.hosts.len());
        &mut self.hosts[id].1
    }
    fn get(&self, host: &str) -> Option<&[IpAddr]> {
        self.index.get(host).map(|&id| {
            assert!(id < self.hosts.len());
            &self.hosts[id].1.ips[..]
        })
    }
    fn get_or_insert(&mut self, host: String) -> &mut Record {
        let id = match self.index.get(&host) {
            Some(id) => *id,
            None => {
                let id = self.hosts.len();
                self.hosts.push((host.clone(), Record::default()));
                self.index.insert(host, id);
                id
            }
        };
        assert!(id < self.hosts.len());
        &mut self.hosts[id].1
    }
    fn len(&self) -> usize {
        self.hosts.len()
    }
    // 是否有ips为空的记录
    fn has_empty(&self) -> bool {
        self.hosts.iter().any(|(_, record)| record.ips.is_empty())
    }
}

struct HostRecordIter<'a> {
    iter: Box<dyn Iterator<Item = &'a mut (String, Record)> + 'a>,
}

impl<'a> HostRecordIter<'a> {
    fn empty_iter<I: Iterator<Item = &'a mut (String, Record)> + 'a>(iter: I) -> Self {
        let iter = iter.filter(|(_, r)| r.empty());
        HostRecordIter {
            iter: Box::new(iter),
        }
    }
    fn new<I: Iterator<Item = &'a mut (String, Record)> + 'a>(iter: I) -> Self {
        HostRecordIter {
            iter: Box::new(iter),
        }
    }
}

impl<'a> Iterator for HostRecordIter<'a> {
    type Item = (&'a str, &'a mut Record);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(k, v)| (k.as_str(), v))
    }
}
unsafe impl<'a> Send for HostRecordIter<'a> {}
unsafe impl<'a> Sync for HostRecordIter<'a> {}
