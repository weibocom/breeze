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
    if let Some(ips) = get_dns().lookup(host) {
        f(ips.as_slice())
    }
}

#[derive(Clone, Debug)]
struct Record {
    subscribers: Vec<Arc<AtomicBool>>,
    ips: Ipv4Vec,
    notify: bool, // true表示需要通知
}
impl Default for Record {
    fn default() -> Self {
        Record {
            subscribers: Vec::with_capacity(2),
            ips: Ipv4Vec::new(),
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
    fn refresh(&mut self, ips: IpAddrLookup) -> bool {
        if ips.len() > 0 && self.ips != ips {
            self.ips = ips;
            self.notify = true;
            true
        } else {
            false
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
    // 每16个tick执行一次empty，避免某一次刷新未解析成功导致需要等待下一个周期；
    // 如果一直有未解析成功的(has_empty()为true)，那么下一个周期仍是全量刷新，而不是仅刷新部分chunk。
    // 其他情况下，每个tick只会刷新部分chunk数据。
    fn iter(&mut self) -> HostRecordIter<'_> {
        // 16是一个经验值。
        if self.hosts.len() > self.last_len || (self.cycle % 16 == 0 && self.hosts.has_empty()) {
            self.last_len = self.hosts.len();
            HostRecordIter::empty_iter(self.hosts.hosts.iter_mut())
        } else {
            // 刷新从上个idx开始的一个chunk长度的数据
            assert!(self.last_len == self.hosts.len());
            let len = self.last_len;
            const PERIOD: usize = 128;
            let ith = self.cycle % PERIOD;
            self.cycle += 1;
            let chunk = (len + (PERIOD - 1)) / PERIOD;
            // 因为chunk是动态变化的，所以不能用last_idx + chunk
            let end = ((ith + 1) * chunk).min(len);
            assert!(
                self.last_idx <= end,
                "addr:{:p} last_idx:{}, end:{}, len:{}, ith:{} chunk:{}",
                &self,
                self.last_idx,
                end,
                len,
                ith,
                chunk,
            );
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
    fn lookup(&self, host: &str) -> Option<&Ipv4Vec> {
        self.hosts.get(host)
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
    fn get(&self, host: &str) -> Option<&Ipv4Vec> {
        self.index.get(host).map(|&id| {
            assert!(id < self.hosts.len());
            &self.hosts[id].1.ips
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

// 一个高效的ipv4数组。
// 小于等于5个元素时，直接使用cache，大于5个元素时，使用ext。
use std::net::Ipv4Addr;
#[derive(Debug, Clone)]
pub struct Ipv4Vec {
    len: u32,
    cache: [Ipv4Addr; 5],
    ext: Option<Box<Vec<Ipv4Addr>>>,
}
impl Ipv4Vec {
    pub fn new() -> Self {
        Self {
            len: 0,
            cache: [Ipv4Addr::from(0u32); 5],
            ext: None,
        }
    }
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len as usize
    }
    pub fn push(&mut self, ip: Ipv4Addr) {
        if self.len() < self.cache.len() {
            self.cache[self.len()] = ip;
        } else {
            let ext = self
                .ext
                .get_or_insert_with(|| Box::new(Vec::with_capacity(16)));
            if ext.len() == 0 {
                ext.extend_from_slice(&self.cache[..])
            }
            ext.push(ip);
        }
        self.len += 1;
    }
    pub fn iter(&self) -> Ipv4VecIter {
        let iter = if self.len() <= self.cache.len() {
            self.cache[..self.len as usize].iter()
        } else {
            self.ext.as_ref().expect("ext").iter()
        };
        Ipv4VecIter { iter }
    }
    fn md5(&self) -> u64 {
        self.iter().fold(0u64, |acc, ip| acc + u32::from(ip) as u64)
    }
    fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn as_slice(&self) -> &[Ipv4Addr] {
        if self.len() <= self.cache.len() {
            &self.cache[..self.len as usize]
        } else {
            self.ext.as_ref().expect("ext").as_slice()
        }
    }
}
pub struct Ipv4VecIter<'a> {
    iter: std::slice::Iter<'a, Ipv4Addr>,
}
impl<'a> Iterator for Ipv4VecIter<'a> {
    type Item = Ipv4Addr;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|ip| *ip)
    }
}
// 为Ipv4Vec实现一个Equal trait，这样就可以比较两个Ipv4Vec是否相等
impl PartialEq for Ipv4Vec {
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len && {
            match self.len {
                0 => true,
                1 => self.cache[0] == other.cache[0],
                2 => {
                    self.cache[..2] == other.cache[..2]
                        || (self.cache[0] == other.cache[1] && self.cache[1] == other.cache[0])
                }
                _ => self.md5() == other.md5(),
            }
        }
    }
}
