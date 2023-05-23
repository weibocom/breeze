use std::{collections::HashMap, marker::PhantomData};

use crate::{dns::DnsConfig, Builder, Endpoint, Timeout, Topology};
use discovery::{
    dns::{self, IPPort},
    TopologyWrite,
};
use protocol::{Protocol, Request, Resource};
use sharding::{
    distribution::Range,
    hash::{Crc32, Hash, HashKey},
    Distance,
};

use super::config::PhantomNamespace;

#[derive(Clone)]
pub struct PhantomService<B, E, Req, P> {
    // 一般有2组，相互做HA，每组是一个域名列表，域名下只有一个ip，但会变化
    streams: Vec<Distance<(String, E)>>,
    hasher: Crc32,
    distribution: Range,
    parser: P,
    cfg: Box<DnsConfig<PhantomNamespace>>,
    _mark: PhantomData<(B, Req)>,
}

impl<B, E, Req, P> From<P> for PhantomService<B, E, Req, P> {
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

impl<B, E, Req, P> Hash for PhantomService<B, E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
    B: Send + Sync,
{
    #[inline]
    fn hash<K: HashKey>(&self, k: &K) -> i64 {
        self.hasher.hash(k)
    }
}

impl<B, E, Req, P> Topology for PhantomService<B, E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
    B: Send + Sync,
{
    // #[inline]
    // fn hash<K: HashKey>(&self, k: &K) -> i64 {
    //     self.hasher.hash(k)
    // }
}

impl<B, E, Req, P> Endpoint for PhantomService<B, E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
    B: Send + Sync,
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
        e.1.send(req)
    }
}

impl<B, E, Req, P> TopologyWrite for PhantomService<B, E, Req, P>
where
    B: Builder<P, Req, E>,
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
    fn load(&mut self) {
        // 先改通知状态，再load，如果失败改一个通用状态，确保下次重试，同时避免变更过程中新的并发变更，待讨论 fishermen
        self.cfg.load_guard().check_load(|| self.load_inner());
    }
}

impl<B, E, Req, P> PhantomService<B, E, Req, P>
where
    B: Builder<P, Req, E>,
    P: Protocol,
    E: Endpoint<Item = Req>,
{
    #[inline]
    fn take_or_build(&self, old: &mut HashMap<String, Vec<E>>, addr: &str, timeout: Timeout) -> E {
        match old.get_mut(addr).map(|endpoints| endpoints.pop()) {
            Some(Some(end)) => end,
            _ => B::build(
                &addr,
                self.parser.clone(),
                Resource::Redis,
                &self.cfg.service,
                timeout,
            ),
        }
    }

    #[inline]
    fn load_inner(&mut self) -> bool {
        let mut addrs = Vec::with_capacity(self.cfg.shards_url.len());
        for shard in self.cfg.shards_url.iter() {
            if shard.is_empty() {
                log::warn!("{:?} shard is empty", self);
                return false;
            }
            let mut shard_ips = Vec::with_capacity(shard.len());
            for url_port in shard.iter() {
                let host = url_port.host();
                dns::lookup_ips(host, |ips| {
                    for ip in ips {
                        shard_ips.push(ip.to_string() + ":" + url_port.port());
                    }
                });
                if shard_ips.len() == 0 {
                    log::info!("dns not inited => {}", url_port);
                    return false;
                }
            }
            assert!(!shard_ips.is_empty());
            addrs.push(shard_ips);
        }

        let old_streams = self.streams.split_off(0);
        self.streams.reserve(old_streams.len());
        let mut old = HashMap::with_capacity(old_streams.len() * 8);

        for mut shard in old_streams {
            for (addr, e) in shard.take() {
                old.entry(addr).or_insert(Vec::new()).push(e);
            }
        }

        for a in addrs.iter() {
            let mut shard_streams = Vec::with_capacity(a.len());
            for addr in a {
                let shard = self.take_or_build(&mut old, addr.as_str(), self.cfg.timeout());
                shard_streams.push((addr.clone(), shard));
            }

            let shard = Distance::from(shard_streams);

            self.streams.push(shard);
        }

        true
    }
}

impl<B, E, Req, P> discovery::Inited for PhantomService<B, E, Req, P>
where
    E: discovery::Inited,
{
    // 每一个域名都有对应的endpoint，并且都初始化完成。
    #[inline]
    fn inited(&self) -> bool {
        self.streams.len() > 0
            && self.streams.len() == self.cfg.shards_url.len()
            && self.streams.iter().fold(true, |inited, shard| {
                inited && {
                    // 每个shard都有对应的endpoint，并且都初始化完成。
                    shard
                        .iter()
                        .fold(true, |inited, (_, e)| inited && e.inited())
                }
            })
    }
}
impl<B, E, Req, P> std::fmt::Debug for PhantomService<B, E, Req, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.cfg)
    }
}
