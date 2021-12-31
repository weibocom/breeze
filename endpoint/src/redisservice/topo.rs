use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use discovery::TopologyWrite;
use protocol::{Builder, Endpoint, Protocol, Request, Resource, Topology};
use sharding::distribution::DIST_RANGE_SPLIT_DEFAULT;
use sharding::hash::Hasher;

use super::config::RedisNamespace;
use discovery::dns::{self, IPPort};
#[derive(Clone)]
pub struct RedisService<B, E, Req, P> {
    // 一共shards.len()个分片，每个分片 shard[0]是master, shard[1..]是slave
    shards: Vec<Vec<(String, E)>>,
    r_idx: Vec<Arc<AtomicUsize>>, // 选择哪个从读取数据
    // 不同sharding的url。第0个是master
    shards_url: Vec<Vec<String>>,
    hasher: Hasher,
    updated: HashMap<String, Arc<AtomicBool>>,
    parser: P,
    service: String,
    _mark: std::marker::PhantomData<(B, Req)>,
}
impl<B, E, Req, P> From<P> for RedisService<B, E, Req, P> {
    #[inline]
    fn from(parser: P) -> Self {
        Self {
            parser,
            shards: Default::default(),
            r_idx: Default::default(),
            shards_url: Default::default(),
            hasher: Default::default(),
            updated: Default::default(),
            service: Default::default(),
            _mark: Default::default(),
        }
    }
}
impl<B, E, Req, P> Topology for RedisService<B, E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
    B: Send + Sync,
{
    #[inline]
    fn hasher(&self) -> &Hasher {
        &self.hasher
    }
}

impl<B: Send + Sync, E, Req, P> protocol::Endpoint for RedisService<B, E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
{
    type Item = Req;
    #[inline(always)]
    fn send(&self, mut req: Self::Item) {
        debug_assert_ne!(self.shards.len(), 0);
        // TODO：原分布算法计算有问题，先临时实现，验证流程 fishermen
        // let shard_idx = req.hash() as usize % self.shards.len();
        let newhash = req
            .hash()
            .wrapping_div(DIST_RANGE_SPLIT_DEFAULT)
            .wrapping_rem(DIST_RANGE_SPLIT_DEFAULT);
        debug_assert!(newhash >= 0);
        let interval = DIST_RANGE_SPLIT_DEFAULT as u64 / self.shards.len() as u64;
        let shard_idx = (newhash as u64 / interval) as usize;

        let shard = unsafe { self.shards.get_unchecked(shard_idx) };
        // log::debug!("+++ shard_idx:{}, req.hash: {}", shard_idx, req.hash());
        let mut idx = 0;
        // 如果有从，并且是读请求，如果目标server异常，会重试其他slave节点
        if shard.len() >= 2 && !req.operation().is_store() {
            debug_assert_eq!(self.shards.len(), self.r_idx.len());
            let ctx = *req.context_mut();
            // 高4个字节是第一次的索引
            // 低4个字节是下一次读取的索引
            let (mut first, mut next) = (ctx >> 32, ctx & 0xffffffff);
            let seq;
            if ctx == 0 {
                seq = unsafe {
                    self.r_idx
                        .get_unchecked(shard_idx)
                        .fetch_add(1, Ordering::Relaxed)
                        & 65535
                };
            } else {
                seq = next as usize;
            }
            // shard 第一个元素是master，默认情况下需要规避
            // TODO: 但是如果所有slave失败，需要访问master，这个逻辑后续需要来加上 fishermen
            idx = seq % (shard.len() - 1) + 1;
            if first == 0 {
                first = idx as u64;
                next = idx as u64 + 1;
            } else {
                next = seq as u64 + 1;
            }

            *req.context_mut() = (first << 32) | next;

            // 减一，是把主减掉
            req.try_next((next - first) < shard.len() as u64 - 1);
        }

        debug_assert!(idx < shard.len());
        shard[idx].1.send(req);
    }
}
impl<B, E, Req, P> TopologyWrite for RedisService<B, E, Req, P>
where
    B: Builder<P, Req, E>,
    P: Protocol,
    E: Endpoint<Item = Req>,
{
    #[inline]
    fn update(&mut self, namespace: &str, cfg: &str) {
        match serde_yaml::from_str::<RedisNamespace>(cfg) {
            Err(e) => log::info!("failed to parse redis namespace:{},{:?}", namespace, e),
            Ok(ns) => {
                self.hasher = Hasher::from(&ns.basic.hash);
                log::info!("update redis cfg: {:?}", cfg);
                let mut shards_url = Vec::new();
                let mut read_idx = Vec::with_capacity(ns.backends.len());
                for shard in ns.backends.iter() {
                    let rd = rand::random::<u32>() as usize;
                    read_idx.push(Arc::new(AtomicUsize::from(rd)));
                    let mut shard_url = Vec::new();
                    for url_port in shard.split(",") {
                        // 注册域名。后续可以通常lookup进行查询。
                        let host = url_port.host();
                        if !self.updated.contains_key(host) {
                            let notify: Arc<AtomicBool> = Arc::default();
                            self.updated.insert(host.to_string(), notify.clone());
                            dns::register(host, notify);
                        }
                        shard_url.push(url_port.to_string());
                    }
                    shards_url.push(shard_url);
                }
                if self.shards_url.len() > 0 {
                    log::info!("top updated from {:?} to {:?}", self.shards_url, shards_url);
                }
                self.shards_url = shards_url;
                self.r_idx = read_idx;
            }
        }
        self.service = namespace.to_string();
    }
    // 满足以下两个条件之一，则需要更新：
    // 1. 存在某dns未成功解析，并且dns数据准备就绪
    // 2. 近期有dns更新。
    #[inline]
    fn need_load(&self) -> bool {
        self.shards.len() != self.shards_url.len()
            || self
                .updated
                .iter()
                .fold(false, |acc, (_k, v)| acc || v.load(Ordering::Acquire))
    }
    #[inline]
    fn load(&mut self) {
        // 把通知放在最前面，避免丢失通知。
        for (_, updated) in self.updated.iter() {
            updated.store(false, Ordering::Release);
        }

        let old_streams = self.shards.split_off(0);
        let mut old = HashMap::with_capacity(old_streams.len());
        for shard in old_streams {
            for (addr, endpoint) in shard {
                // 一个ip可能存在于多个域名中。
                old.entry(addr).or_insert(Vec::new()).push(endpoint);
            }
        }
        // 遍历所有的shards_url
        for shard in self.shards_url.iter() {
            let mut endpoints = Vec::new();
            if shard.len() > 0 {
                let master_url = &shard[0];
                let masters = dns::lookup_ips(master_url.host());
                if masters.len() > 0 {
                    // 忽略多个masterip的情况
                    let master_ip = &masters[0];
                    let master_addr = master_ip.to_string() + ":" + master_url.port();
                    let timeout = Duration::from_millis(500);
                    let master = self.take_or_build(&mut old, &master_addr, timeout);
                    endpoints.push((master_addr, master));

                    // slave
                    for url_port in &shard[1..] {
                        let url = url_port.host();
                        let port = url_port.port();
                        for slave_ip in dns::lookup_ips(url) {
                            let addr = slave_ip + ":" + port;
                            let timeout = Duration::from_millis(200);
                            let slave = self.take_or_build(&mut old, &addr, timeout);
                            endpoints.push((addr, slave));
                        }
                    }
                }
            }
            // 无论怎样，需要都要将endpoints push进去。需要占位，保证shard位置与url中的位置对齐
            self.shards.push(endpoints);
        }

        log::info!("loading complete {}", self.service);
    }
}
impl<B, E, Req, P> discovery::Inited for RedisService<B, E, Req, P>
where
    E: discovery::Inited,
{
    // 每一个域名都有对应的endpoint，并且都初始化完成。
    #[inline]
    fn inited(&self) -> bool {
        // 每一个分片都有初始, 并且至少有一主一从。
        self.shards.len() == self.shards_url.len()
            && self.shards.iter().fold(true, |inited, shard| {
                inited && shard.len() >= 2 && shard.iter().fold(true, |i, e| i && e.1.inited())
            })
    }
}
impl<B, E, Req, P> RedisService<B, E, Req, P>
where
    B: Builder<P, Req, E>,
    P: Protocol,
    E: Endpoint<Item = Req>,
{
    #[inline]
    fn take_or_build(&self, old: &mut HashMap<String, Vec<E>>, addr: &str, timeout: Duration) -> E {
        match old.get_mut(addr).map(|endpoints| endpoints.pop()) {
            Some(Some(end)) => end,
            _ => B::build(
                &addr,
                self.parser.clone(),
                Resource::Redis,
                &self.service,
                timeout,
            ),
        }
    }
}
