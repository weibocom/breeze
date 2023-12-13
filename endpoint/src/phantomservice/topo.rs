use std::marker::PhantomData;

use crate::{
    dns::{DnsConfig, DnsLookup},
    select::Distance,
    Endpoint, Endpoints, Topology,
};
use discovery::{Inited, TopologyWrite};
use protocol::{Protocol, Request, Resource::Phantom};
use sharding::{
    distribution::Range,
    hash::{Crc32, Hash, HashKey},
};

use super::config::PhantomNamespace;

#[derive(Clone)]
pub struct PhantomService<E, Req, P> {
    // 一般有2组，相互做HA，每组是一个域名列表，域名下只有一个ip，但会变化
    streams: Vec<Distance<E>>,
    hasher: Crc32,
    distribution: Range,
    parser: P,
    cfg: Box<DnsConfig<PhantomNamespace>>,
    _mark: PhantomData<Req>,
}

impl<E, Req, P> From<P> for PhantomService<E, Req, P> {
    fn from(parser: P) -> Self {
        Self {
            parser,
            streams: Default::default(),
            hasher: Default::default(),
            distribution: Default::default(),
            cfg: Default::default(),
            _mark: Default::default(),
        }
    }
}

impl<E, Req, P> Hash for PhantomService<E, Req, P>
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

impl<E, Req, P> Topology for PhantomService<E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
{
}

impl<E, Req, P> Endpoint for PhantomService<E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
{
    type Item = Req;
    #[inline]
    fn send(&self, mut req: Self::Item) {
        debug_assert_ne!(self.streams.len(), 0);

        // 确认分片idx
        let s_idx = self.distribution.index(req.hash());
        debug_assert!(s_idx < self.streams.len(), "{} {:?} {:?}", s_idx, self, req);
        let shard = unsafe { self.streams.get_unchecked(s_idx) };

        let mut ctx = super::Context::from(*req.context_mut());
        let idx = ctx.fetch_add_idx(); // 按顺序轮询
                                       // 写操作，写所有实例
        req.write_back(req.operation().is_store() && ctx.index() < shard.len());
        // 读操作，只重试一次
        req.try_next(idx == 0);
        //ctx.update_idx(idx);
        assert!(idx < shard.len(), "{} {:?} {:?}", idx, self, req);
        let e = unsafe { shard.get_unchecked(idx) };
        //ctx.check_inited();
        *req.context_mut() = ctx.ctx;
        e.send(req)
    }
}

impl<E, Req, P> TopologyWrite for PhantomService<E, Req, P>
where
    P: Protocol,
    E: Endpoint<Item = Req>,
{
    #[inline]
    fn update(&mut self, namespace: &str, cfg: &str) {
        if let Some(ns) = PhantomNamespace::try_from(cfg) {
            log::info!("topo updating {:?} => {:?}", self, ns);
            // phantome 只会使用crc32
            //self.hasher = Hasher::from(&ns.basic.hash);
            let dist = &ns.basic.distribution;
            let num = dist
                .find('-')
                .map(|idx| dist[idx + 1..].parse::<u64>().ok())
                .flatten();
            self.distribution = Range::from(num, ns.backends.len());

            self.cfg.update(namespace, ns);
        }
    }

    // 更新条件：
    //   1. 最近存在dns解析失败；
    //   2. 近期有dns更新；
    #[inline]
    fn need_load(&self) -> bool {
        self.streams.len() != self.cfg.shards_url.len() || self.cfg.need_load()
    }
    #[inline]
    fn load(&mut self) -> bool {
        // 先改通知状态，再load，如果失败改一个通用状态，确保下次重试，同时避免变更过程中新的并发变更，待讨论 fishermen
        self.cfg
            .load_guard()
            .check_load(|| self.load_inner().is_some())
    }
}

impl<E, Req, P> PhantomService<E, Req, P>
where
    P: Protocol,
    E: Endpoint<Item = Req>,
{
    #[inline]
    fn load_inner(&mut self) -> Option<()> {
        let addrs = self.cfg.shards_url.lookup()?;
        assert_eq!(addrs.len(), self.cfg.shards_url.len());
        let mut endpoints: Endpoints<'_, Req, P, E> =
            Endpoints::new(&self.cfg.service, &self.parser, Phantom);
        // 把老的stream缓存起来
        self.streams.split_off(0).into_iter().for_each(|shard| {
            endpoints.cache(shard.into_inner());
        });
        addrs.iter().for_each(|shard| {
            assert!(!shard.is_empty());
            let backends = endpoints.take_or_build(&*shard, self.cfg.timeout());
            self.streams.push(Distance::from(backends));
        });
        // endpoints中如果还有stream，会被drop掉
        Some(())
    }
}

impl<E: Inited, Req, P> Inited for PhantomService<E, Req, P> {
    // 每一个域名都有对应的endpoint，并且都初始化完成。
    #[inline]
    fn inited(&self) -> bool {
        self.streams.len() > 0
            && self.streams.len() == self.cfg.shards_url.len()
            && self.streams.iter().fold(true, |inited, shard| {
                inited && {
                    // 每个shard都有对应的endpoint，并且都初始化完成。
                    shard.iter().fold(true, |inited, e| inited && e.inited())
                }
            })
    }
}
impl<E, Req, P> std::fmt::Debug for PhantomService<E, Req, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.cfg)
    }
}
