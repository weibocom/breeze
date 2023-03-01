use std::{
    collections::HashMap,
    marker::PhantomData,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use crate::{Builder, Endpoint, Topology};
use discovery::{
    dns::{self, IPPort},
    TopologyWrite,
};
use protocol::{Protocol, Request, Resource};
use sharding::{distribution::Range, hash::Hasher, Distance};

use super::config::PhantomNamespace;
use crate::TimeoutAdjust;

const CONFIG_UPDATED_KEY: &str = "__config__";

#[derive(Clone)]
pub struct PhantomService<B, E, Req, P> {
    // 一般有2组，相互做HA，每组是一个域名列表，域名下只有一个ip，但会变化
    streams: Vec<Distance<(String, E)>>,
    // m * n: m个shard，每个shard有n个ip
    streams_backend: Vec<Vec<String>>,
    updated: HashMap<String, Arc<AtomicBool>>,
    hasher: Hasher,
    distribution: Range,
    parser: P,
    service: String,
    timeout: Duration,
    _mark: PhantomData<(B, Req)>,
}

impl<B, E, Req, P> From<P> for PhantomService<B, E, Req, P> {
    fn from(parser: P) -> Self {
        Self {
            parser,
            streams: Default::default(),
            streams_backend: Default::default(),
            updated: Default::default(),
            hasher: Default::default(),
            service: Default::default(),
            timeout: crate::TO_PHANTOM_M,
            distribution: Default::default(),
            _mark: Default::default(),
        }
    }
}

impl<B, E, Req, P> Topology for PhantomService<B, E, Req, P>
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
        let idx = self.distribution.index(req.hash());
        assert!(idx < self.streams.len(), "{} {:?} {:?}", idx, self, req);
        let shard = unsafe { self.streams.get_unchecked(idx) };
        // 低8位是索引，最高bit表示是否初始化
        let mut ctx = super::Context::from(*req.context_mut());

        let e = if req.operation().is_store() {
            let idx = ctx.fetch_add_idx();
            req.write_back(ctx.index() < shard.len());
            assert!(idx < shard.len(), "{} {:?} {:?}", idx, self, req);
            unsafe { shard.get_unchecked(idx) }
        } else {
            // 读请求。
            if !ctx.inited() {
                let (idx, e) = shard.unsafe_select();
                req.try_next(shard.len() > 1);
                ctx.update_idx(idx);
                e
            } else {
                req.try_next(false);
                assert!(ctx.index() < shard.len(), "{} {:?} {:?}", idx, self, req);
                unsafe { shard.unsafe_next(ctx.index(), 1).1 }
            }
        };
        ctx.check_inited();
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
        self.service = namespace.to_string();
        if let Some(ns) = PhantomNamespace::try_from(cfg) {
            log::info!("topo updating {:?} => {:?}", self, ns);
            self.timeout.adjust(ns.basic.timeout_ms);
            self.hasher = Hasher::from(&ns.basic.hash);
            self.distribution = Range::from(&ns.basic.distribution, ns.backends.len());
            self.service = namespace.to_string();

            self.streams_backend = ns
                .backends
                .iter()
                .map(|v| v.split(',').map(|s| s.to_string()).collect())
                .collect();
            for b in self.streams_backend.iter() {
                for url_port in b {
                    let host = url_port.host();
                    if !self.updated.contains_key(host) {
                        let watcher = dns::register(host);
                        self.updated.insert(host.to_string(), watcher);
                    }
                }
            }

            // 配置更新完毕，如果watcher确认配置update了，各个topo就重新进行load
            self.updated
                .entry(CONFIG_UPDATED_KEY.to_string())
                .or_insert(Arc::new(AtomicBool::new(true)))
                .store(true, Ordering::Release);
        }
    }

    // 更新条件：
    //   1. 最近存在dns解析失败；
    //   2. 近期有dns更新；
    #[inline]
    fn need_load(&self) -> bool {
        self.streams.len() != self.streams_backend.len()
            || self
                .updated
                .iter()
                .fold(false, |acc, (_k, v)| acc || v.load(Ordering::Acquire))
    }
    #[inline]
    fn load(&mut self) {
        // 先改通知状态，再load，如果失败改一个通用状态，确保下次重试，同时避免变更过程中新的并发变更，待讨论 fishermen
        for (_, updated) in self.updated.iter() {
            updated.store(false, Ordering::Release);
        }

        // 根据最新配置更新topo，如果更新失败，将CONFIG_UPDATED_KEY设为true，强制下次重新加载
        let succeed = self.load_inner();
        if !succeed {
            self.updated
                .get_mut(CONFIG_UPDATED_KEY)
                .expect("phantom config state missed")
                .store(true, Ordering::Release);
            log::warn!("phantom will reload topo later...");
        }
    }
}

impl<B, E, Req, P> PhantomService<B, E, Req, P>
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

    #[inline]
    fn load_inner(&mut self) -> bool {
        let mut addrs = Vec::with_capacity(self.streams_backend.len());
        for shard in self.streams_backend.iter() {
            if shard.is_empty() {
                log::warn!("{:?} shard is empty", self);
                return false;
            }
            let mut shard_ips = Vec::with_capacity(shard.len());
            for url_port in shard.iter() {
                let host = url_port.host();
                let ips = dns::lookup_ips(host);
                if ips.len() == 0 {
                    log::warn!("phantom dns looked up failed for {} => {:?}", host, self);
                    return false;
                }
                for ip in ips {
                    shard_ips.push(ip + ":" + url_port.port());
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
                let shard = self.take_or_build(&mut old, addr.as_str(), self.timeout);
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
            && self.streams.len() == self.streams_backend.len()
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
        write!(
            f,
            "service:{} backends:{:?} timeout:{:?}",
            self.service, self.streams_backend, self.timeout
        )
    }
}
