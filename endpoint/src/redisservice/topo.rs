use ds::time::Duration;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::{Builder, Endpoint, Single, Topology};
use discovery::TopologyWrite;
use protocol::{Protocol, Request, Resource};
use sharding::distribution::Distribute;
use sharding::hash::Hasher;
use sharding::{ReplicaSelect, Selector};

use super::config::RedisNamespace;
use crate::TimeoutAdjust;
use discovery::dns::{self, IPPort};

const CONFIG_UPDATED_KEY: &str = "__redis_config__";
#[derive(Clone)]
pub struct RedisService<B, E, Req, P> {
    // 一共shards.len()个分片，每个分片 shard[0]是master, shard[1..]是slave
    shards: Vec<Shard<E>>,
    // 不同sharding的url。第0个是master
    shards_url: Vec<Vec<String>>,
    hasher: Hasher,
    distribute: Distribute,
    selector: Selector, // 从的选择策略。
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
            selector: Selector::Random,
            timeout_master: crate::TO_REDIS_M,
            timeout_slave: crate::TO_REDIS_S,
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

impl<B: Send + Sync, E, Req, P> Endpoint for RedisService<B, E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
{
    type Item = Req;
    #[inline]
    fn send(&self, mut req: Self::Item) {
        debug_assert_ne!(self.shards.len(), 0);

        use protocol::RedisFlager;
        let shard_idx = match req.cmd().direct_hash() {
            true => {
                let dhash_boundary = protocol::MAX_DIRECT_HASH - self.shards.len() as i64;
                // 大于direct hash边界，说明是全节点分发请求，发送请求前，需要调整hash为下次发送做准备
                // 备注：正常计算的hash范围是u32；
                if req.hash() > dhash_boundary {
                    // 边界之上的hash，其idx为max减去对应hash值，从而轮询所有分片
                    let idx = (protocol::MAX_DIRECT_HASH - req.hash()) as usize;

                    // req的hash自减1，同时设置write back，为下一次write back分发做准备
                    req.update_hash(req.hash() - 1);
                    req.write_back(req.hash() > dhash_boundary);
                    idx
                } else {
                    self.distribute.index(req.hash())
                }
            }
            false => self.distribute.index(req.hash()),
        };

        debug_assert!(
            shard_idx < self.shards.len(),
            "redis: {}/{} req:{:?}",
            shard_idx,
            self.shards.len(),
            req
        );
        let shard = unsafe { self.shards.get_unchecked(shard_idx) };
        log::debug!("+++ {} send {} => {:?}", self.service, shard_idx, req);

        // 如果有从，并且是读请求，如果目标server异常，会重试其他slave节点
        if shard.has_slave() && !req.operation().is_store() && !req.cmd().master_only() {
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
            //let try_next = ctx.runs == 1 || (ctx.runs as usize) < shard.slaves.len();
            // 只重试一次，重试次数过多，可能会导致雪崩。
            let try_next = ctx.runs == 1;
            req.try_next(try_next);

            endpoint.1.send(req)
        } else {
            shard.master().send(req)
        }
    }

    #[inline]
    fn shard_idx(&self, hash: i64) -> usize {
        self.distribute.index(hash)
    }
}
impl<B, E, Req, P> TopologyWrite for RedisService<B, E, Req, P>
where
    B: Builder<P, Req, E>,
    P: Protocol,
    E: Endpoint<Item = Req> + Single,
{
    #[inline]
    fn update(&mut self, namespace: &str, cfg: &str) {
        if let Some(ns) = RedisNamespace::try_from(cfg) {
            self.timeout_master.adjust(ns.basic.timeout_ms_master);
            self.timeout_slave.adjust(ns.basic.timeout_ms_slave);
            self.hasher = Hasher::from(&ns.basic.hash);
            self.distribute = Distribute::from(ns.basic.distribution.as_str(), &ns.backends);
            self.selector = ns.basic.selector.as_str().into();

            // TODO 直接设置selector属性，在更新最后，设置全局更新标志，deadcode暂时保留，观察副作用 2022.12后可以删除
            // selector属性更新与域名实例更新保持一致
            // if self.selector != ns.basic.selector {
            //     self.selector = ns.basic.selector;
            //     self.updated
            //         .entry(CONFIG_UPDATED_KEY.to_string())
            //         .or_insert(Arc::new(AtomicBool::new(true)))
            //         .store(true, Ordering::Release);
            // }

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

            // 配置更新完毕，如果watcher确认配置update了，各个topo就重新进行load
            self.updated
                .entry(CONFIG_UPDATED_KEY.to_string())
                .or_insert(Arc::new(AtomicBool::new(true)))
                .store(true, Ordering::Release);
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
        // TODO: 先改通知状态，再load，如果失败，改一个通用状态，确保下次重试，同时避免变更过程中新的并发变更，待讨论 fishermen
        for (_, updated) in self.updated.iter() {
            updated.store(false, Ordering::Release);
        }

        // 根据最新配置更新topo，如果更新失败，将CONFIG_UPDATED_KEY设为true，强制下次重新加载
        let succeed = self.load_inner();
        if !succeed {
            self.updated
                .get_mut(CONFIG_UPDATED_KEY)
                .expect("redis config state missed")
                .store(true, Ordering::Release);
            log::warn!("redis will reload topo later...");
        }
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
    E: Endpoint<Item = Req> + Single,
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
                Default::default(),
            ),
        }
    }

    // TODO 把load的日志级别提升，在罕见异常情况下（dns解析异常、配置异常）,持续load时可以通过日志来跟进具体状态；
    //      当然，也可以通过指标汇报的方式进行，但对这种罕见情况进行metrics消耗，需要考量；
    //      先对这种罕见情况用日志记录，确有需要，再考虑用指标汇报； 待讨论 fishermen
    #[inline]
    fn load_inner(&mut self) -> bool {
        // 所有的ip要都能解析出主从域名
        let mut addrs = Vec::with_capacity(self.shards_url.len());
        for shard in self.shards_url.iter() {
            if shard.len() < 2 {
                log::warn!("{} both master and slave required.", self.service);
                return false;
            }
            let master_url = &shard[0];
            let masters = dns::lookup_ips(master_url.host());
            if masters.len() == 0 {
                log::warn!("{} master not looked up", master_url);
                return false;
            }
            if masters.len() > 1 {
                log::warn!("multi master ip parsed. {} => {:?}", master_url, masters);
            }
            let master = String::from(&masters[0]) + ":" + master_url.port();
            let mut slaves = Vec::with_capacity(8);
            for url_port in &shard[1..] {
                let url = url_port.host();
                let port = url_port.port();
                for slave_ip in dns::lookup_ips(url) {
                    let addr = slave_ip + ":" + port;
                    if !slaves.contains(&addr) {
                        slaves.push(addr);
                    }
                }
            }
            if slaves.len() == 0 {
                log::warn!("{:?} slave not looked up", &shard[1..]);
                return false;
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
            master.enable_single();

            // slave
            let mut replicas = Vec::with_capacity(8);
            for addr in slaves {
                let slave = self.take_or_build(&mut old, &addr, self.timeout_slave);
                slave.disable_single();
                replicas.push((addr, slave));
            }
            let shard = Shard::selector(self.selector, master_addr, master, replicas);
            self.shards.push(shard);
        }
        assert_eq!(
            self.shards.len(),
            self.shards_url.len(),
            "shards/urs: {}/{}",
            self.shards.len(),
            self.shards_url.len()
        );
        log::info!(
            "{} load complete. {} dropping:{:?}",
            self.service,
            self.shards.len(),
            {
                old.retain(|_k, v| v.len() > 0);
                old.keys()
            }
        );

        true
    }
}
#[derive(Clone)]
struct Shard<E> {
    master: (String, E),
    slaves: ReplicaSelect<(String, E)>,
}
impl<E> Shard<E> {
    #[inline]
    fn selector(s: Selector, master_host: String, master: E, replicas: Vec<(String, E)>) -> Self {
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
