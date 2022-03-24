use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use discovery::TopologyWrite;
use protocol::{Builder, Endpoint, Protocol, Request, Resource, Topology};
use sharding::distribution::Distribute;
use sharding::hash::Hasher;
use sharding::ReplicaSelect;

use super::config::RedisNamespace;
use discovery::dns::{self, IPPort};
#[derive(Clone)]
pub struct RedisService<B, E, Req, P> {
    // 一共shards.len()个分片，每个分片 shard[0]是master, shard[1..]是slave
    shards: Vec<Shard<E>>,
    // 不同sharding的url。第0个是master
    shards_url: Vec<Vec<String>>,
    hasher: Hasher,
    distribute: Distribute,
    selector: String, // 从的选择策略。
    updated: HashMap<String, Arc<AtomicBool>>,
    parser: P,
    service: String,
    timeout_master: Duration,
    timeout_slave: Duration,
    _mark: std::marker::PhantomData<(B, Req)>,
}
impl<B, E, Req, P> From<P> for RedisService<B, E, Req, P> {
    #[inline]
    fn from(parser: P) -> Self {
        Self {
            parser,
            shards: Default::default(),
            shards_url: Default::default(),
            hasher: Default::default(),
            distribute: Default::default(),
            updated: Default::default(),
            service: Default::default(),
            selector: Default::default(),
            timeout_master: Duration::from_millis(200),
            timeout_slave: Duration::from_millis(80),
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
    #[inline]
    fn send(&self, mut req: Self::Item) {
        assert_ne!(self.shards.len(), 0);

        let shard_idx = self.distribute.index(req.hash());
        assert!(
            shard_idx < self.shards.len(),
            "{} >= {}  => {:?}",
            shard_idx,
            self.shards.len(),
            req
        );
        let shard = unsafe { self.shards.get_unchecked(shard_idx) };

        // 跟踪hash为0的场景，hash设置错误、潜在bug可能导致hash为0，待2022.12后再考虑清理 fishermen
        use protocol::Utf8;
        if req.hash() == 0 {
            log::warn!(
                "+++ careful - {} hash/idx:{}/{}, req:{:?}",
                self.service,
                req.hash(),
                shard_idx,
                req.data().utf8()
            )
        }

        // 如果有从，并且是读请求，如果目标server异常，会重试其他slave节点
        if shard.has_slave() && !req.operation().is_store() {
            let ctx = super::transmute(req.context_mut());
            let (idx, endpoint) = if ctx.runs == 0 {
                shard.select()
            } else {
                if (ctx.runs as usize) < shard.slaves.len() {
                    shard.next(ctx.idx as usize, ctx.runs as usize)
                } else {
                    // 说明只有一个从，并且从访问失败了，会通过主访问。
                    (ctx.idx as usize, &shard.master)
                }
            };
            ctx.idx = idx as u32;
            ctx.runs += 1;
            // TODO: 但是如果所有slave失败，需要访问master，这个逻辑后续需要来加上 fishermen
            // 1. 第一次访问. （无论如何都允许try_next，如果只有一个从，则下一次失败时访问主）
            // 2. 有多个从，访问的次数小于从的数量
            let try_next = ctx.runs == 1 || (ctx.runs as usize) < shard.slaves.len();
            req.try_next(try_next);

            endpoint.1.send(req)
        } else {
            shard.master().send(req)
        }
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
        if let Some(ns) = RedisNamespace::try_from(cfg) {
            let len = ns.backends.len();
            let power_two = len > 0 && ((len & len - 1) == 0);
            if !power_two {
                log::error!("{} shard num {} is not power of two", self.service, len);
                return;
            }

            self.timeout_master = ns.timeout_master();
            self.timeout_slave = ns.timeout_slave();
            self.hasher = Hasher::from(&ns.basic.hash);
            self.distribute = Distribute::from(ns.basic.distribution.as_str(), &ns.backends);
            self.selector = ns.basic.selector;
            let mut shards_url = Vec::new();
            for shard in ns.backends.iter() {
                let mut shard_url = Vec::new();
                for url_port in shard.split(",") {
                    // 注册域名。后续可以通常lookup进行查询。
                    let host = url_port.host();
                    if !self.updated.contains_key(host) {
                        let watcher = dns::register(host);
                        self.updated.insert(host.to_string(), watcher);
                    }
                    shard_url.push(url_port.to_string());
                }
                shards_url.push(shard_url);
            }
            if self.shards_url.len() > 0 {
                log::debug!("top updated from {:?} to {:?}", self.shards_url, shards_url);
            }
            self.shards_url = shards_url;
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

        // 所有的ip要都能解析出主从域名
        let mut addrs = Vec::with_capacity(self.shards_url.len());
        for shard in self.shards_url.iter() {
            if shard.len() < 2 {
                log::warn!("{} both master and slave required.", self.service);
                return;
            }
            let master_url = &shard[0];
            let masters = dns::lookup_ips(master_url.host());
            if masters.len() == 0 {
                log::debug!("{} master not looked up", master_url);
                return;
            }
            if masters.len() > 1 {
                log::info!("multi master ip parsed. {} => {:?}", master_url, masters);
            }
            let master = String::from(&masters[0]) + ":" + master_url.port();
            let mut slaves = Vec::with_capacity(8);
            for url_port in &shard[1..] {
                let url = url_port.host();
                let port = url_port.port();
                for slave_ip in dns::lookup_ips(url) {
                    let addr = slave_ip + ":" + port;
                    slaves.push(addr);
                }
            }
            if slaves.len() == 0 {
                log::debug!("{:?} slave not looked up", &shard[1..]);
                return;
            }
            addrs.push((master, slaves));
        }
        // 到这之后，所有的shard都能解析出ip

        let mut old = HashMap::with_capacity(self.shards.len());
        for shard in self.shards.split_off(0) {
            old.entry(shard.master.0)
                .or_insert(Vec::new())
                .push(shard.master.1);
            for (addr, endpoint) in shard.slaves.into_inner() {
                // 一个ip可能存在于多个域名中。
                old.entry(addr).or_insert(Vec::new()).push(endpoint);
            }
        }
        // 遍历所有的shards_url
        for (master_addr, slaves) in addrs {
            assert_ne!(master_addr.len(), 0);
            assert_ne!(slaves.len(), 0);
            let master = self.take_or_build(&mut old, &master_addr, self.timeout_master);

            // slave
            let mut replicas = Vec::with_capacity(8);
            for addr in slaves {
                let slave = self.take_or_build(&mut old, &addr, self.timeout_slave);
                replicas.push((addr, slave));
            }
            let shard = Shard::selector(&self.selector, master_addr, master, replicas);
            self.shards.push(shard);
        }
        assert_eq!(self.shards.len(), self.shards_url.len());
        log::debug!(
            "{} load complete. {} dropping:{:?}",
            self.service,
            self.shards.len(),
            {
                old.retain(|_k, v| v.len() > 0);
                old.keys()
            }
        );
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
        self.shards.len() > 0
            && self.shards.len() == self.shards_url.len()
            && self
                .shards
                .iter()
                .fold(true, |inited, shard| inited && shard.inited())
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
#[derive(Clone)]
struct Shard<E> {
    master: (String, E),
    slaves: ReplicaSelect<(String, E)>,
}
impl<E> Shard<E> {
    #[inline]
    fn selector(s: &str, master_host: String, master: E, replicas: Vec<(String, E)>) -> Self {
        Self {
            master: (master_host, master),
            slaves: ReplicaSelect::from(s, replicas),
        }
    }
    #[inline]
    fn has_slave(&self) -> bool {
        self.slaves.len() > 0
    }
    #[inline]
    fn master(&self) -> &E {
        &self.master.1
    }
    #[inline]
    fn select(&self) -> (usize, &(String, E)) {
        unsafe { self.slaves.unsafe_select() }
    }
    #[inline]
    fn next(&self, idx: usize, runs: usize) -> (usize, &(String, E)) {
        unsafe { self.slaves.unsafe_next(idx, runs) }
    }
}
impl<E: discovery::Inited> Shard<E> {
    // 1. 主已经初始化
    // 2. 有从
    // 3. 所有的从已经初始化
    #[inline]
    fn inited(&self) -> bool {
        self.master().inited()
            && self.has_slave()
            && self
                .slaves
                .as_ref()
                .iter()
                .fold(true, |inited, (_, e)| inited && e.inited())
    }
}
