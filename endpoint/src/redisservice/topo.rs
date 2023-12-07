use crate::{
    dns::{DnsConfig, DnsLookup},
    shards::Shard,
    Endpoint, Endpoints, PerformanceTuning, Topology,
};
use discovery::TopologyWrite;
use protocol::{Protocol, RedisFlager, Request, Resource::Redis};
use sharding::distribution::Distribute;
use sharding::hash::{Hash, HashKey, Hasher};

use super::config::RedisNamespace;

#[derive(Clone)]
pub struct RedisService<E, Req, P> {
    // 一共shards.len()个分片，每个分片 shard[0]是master, shard[1..]是slave
    shards: Vec<Shard<E>>,
    hasher: Hasher,
    distribute: Distribute,
    parser: P,
    cfg: Box<DnsConfig<RedisNamespace>>,
    _mark: std::marker::PhantomData<Req>,
}
impl<E, Req, P> From<P> for RedisService<E, Req, P> {
    #[inline]
    fn from(parser: P) -> Self {
        Self {
            parser,
            shards: Default::default(),
            hasher: Default::default(),
            distribute: Default::default(),
            cfg: Default::default(),
            _mark: Default::default(),
        }
    }
}

impl<E, Req, P> Hash for RedisService<E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
{
    #[inline]
    fn hash<K: HashKey>(&self, k: &K) -> i64 {
        self.hasher.hash(k)
    }
}

impl<E, Req, P> Topology for RedisService<E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
{
}

impl<E, Req, P> Endpoint for RedisService<E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
{
    type Item = Req;
    #[inline]
    fn send(&self, mut req: Self::Item) {
        debug_assert_ne!(self.shards.len(), 0);

        let shard_idx = if req.sendto_all() {
            //全节点分发请求
            let ctx = super::transmute(req.context_mut());
            let idx = ctx.shard_idx as usize;
            ctx.shard_idx += 1;
            req.write_back(idx < self.shards.len() - 1);
            idx
        } else {
            self.distribute.index(req.hash())
        };

        assert!(shard_idx < self.len(), "{} {:?} {}", shard_idx, req, self);

        let shard = unsafe { self.shards.get_unchecked(shard_idx) };
        log::debug!("{} send master{} {}=>{:?}", self, req, shard_idx, req);

        // 如果有从，并且是读请求，如果目标server异常，会重试其他slave节点
        if shard.has_slave() && !req.operation().is_store() && !req.master_only() {
            if *req.context_mut() == 0 {
                if let Some(quota) = shard.slaves.quota() {
                    req.quota(quota);
                }
            }
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
            ctx.idx = idx as u16;
            ctx.runs += 1;
            // TODO: 但是如果所有slave失败，需要访问master，这个逻辑后续需要来加上 fishermen
            // 1. 第一次访问. （无论如何都允许try_next，如果只有一个从，则下一次失败时访问主）
            // 2. 有多个从，访问的次数小于从的数量
            //let try_next = ctx.runs == 1 || (ctx.runs as usize) < shard.slaves.len();
            // 只重试一次，重试次数过多，可能会导致雪崩。
            let try_next = ctx.runs == 1;
            req.try_next(try_next);

            endpoint.send(req)
        } else {
            shard.master().send(req)
        }
    }

    #[inline]
    fn shard_idx(&self, hash: i64) -> usize {
        self.distribute.index(hash)
    }
}
impl<E, Req, P> TopologyWrite for RedisService<E, Req, P>
where
    P: Protocol,
    E: Endpoint<Item = Req>,
{
    #[inline]
    fn update(&mut self, namespace: &str, cfg: &str) {
        if let Some(ns) = RedisNamespace::try_from(cfg) {
            self.hasher = Hasher::from(&ns.basic.hash);
            self.distribute = Distribute::from(ns.basic.distribution.as_str(), &ns.backends);
            self.cfg.update(namespace, ns);
        }
    }
    // 满足以下两个条件之一，则需要更新：
    // 1. 存在某dns未成功解析，并且dns数据准备就绪
    // 2. 近期有dns更新。
    #[inline]
    fn need_load(&self) -> bool {
        self.shards.len() != self.cfg.shards_url.len() || self.cfg.need_load()
    }

    #[inline]
    fn load(&mut self) -> bool {
        // TODO: 先改通知状态，再load，如果失败，改一个通用状态，确保下次重试，同时避免变更过程中新的并发变更，待讨论 fishermen
        self.cfg
            .load_guard()
            .check_load(|| self.load_inner().is_some())
    }
}
impl<E, Req, P> discovery::Inited for RedisService<E, Req, P>
where
    E: discovery::Inited,
{
    // 每一个域名都有对应的endpoint，并且都初始化完成。
    #[inline]
    fn inited(&self) -> bool {
        // 每一个分片都有初始, 并且至少有一主一从。
        self.shards.len() > 0
            && self.shards.len() == self.cfg.shards_url.len()
            && self
                .shards
                .iter()
                .fold(true, |inited, shard| inited && shard.inited())
    }
}
impl<E, Req, P> RedisService<E, Req, P> {
    #[inline]
    fn len(&self) -> usize {
        self.shards.len()
    }
}

impl<E, Req, P> RedisService<E, Req, P>
where
    P: Protocol,
    E: Endpoint<Item = Req>,
{
    // TODO 把load的日志级别提升，在罕见异常情况下（dns解析异常、配置异常）,持续load时可以通过日志来跟进具体状态；
    //      当然，也可以通过指标汇报的方式进行，但对这种罕见情况进行metrics消耗，需要考量；
    //      先对这种罕见情况用日志记录，确有需要，再考虑用指标汇报； 待讨论 fishermen
    #[inline]
    fn load_inner(&mut self) -> Option<()> {
        let addrs = self.cfg.shards_url.master_lookup()?;
        assert_eq!(addrs.len(), self.cfg.shards_url.len());
        // 到这之后，所有的shard都能解析出ip

        // 把所有的endpoints cache下来
        let mut endpoints: Endpoints<'_, Req, P, E> =
            Endpoints::new(&self.cfg.service, &self.parser, Redis);
        self.shards.split_off(0).into_iter().for_each(|shard| {
            endpoints.cache_one(shard.master);
            endpoints.cache(shard.slaves.into_inner());
        });

        // 遍历所有的shards_url
        addrs.iter().for_each(|ips| {
            assert!(ips.len() >= 2);
            let master = endpoints.take_or_build_one(&ips[0], self.cfg.timeout_master());
            // 第0个是master，如果master提供读，则从第0个开始。
            let oft = if self.cfg.basic.master_read { 0 } else { 1 };
            let slaves = endpoints.take_or_build(&ips[oft..], self.cfg.timeout_slave());
            let shard = Shard::selector(
                self.cfg.basic.selector.tuning_mode(),
                master,
                slaves,
                self.cfg.basic.region_enabled,
            );
            let ty = &*self.cfg.basic.resource_type;
            shard.check_region_len(ty, &self.cfg.service);
            self.shards.push(shard);
        });
        Some(())
    }
}

impl<E, Req, P> std::fmt::Display for RedisService<E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisService")
            .field("cfg", &self.cfg)
            .field("shards", &self.shards.len())
            .finish()
    }
}
