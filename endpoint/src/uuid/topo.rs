use std::collections::HashMap;

use crate::{Builder, Endpoint, Single, Topology};
use discovery::TopologyWrite;
use protocol::{Protocol, Request, Resource};
use sharding::{
    hash::{Hash, HashKey},
    Distance,
};

use super::config::UuidNamespace;
use crate::{dns::DnsConfig, Timeout};
use discovery::dns::{self, IPPort};

#[derive(Clone)]
pub struct UuidService<B, E, Req, P> {
    backends: Distance<(String, E)>,
    parser: P,
    cfg: Box<DnsConfig<UuidNamespace>>,
    _mark: std::marker::PhantomData<(B, Req)>,
}
impl<B, E, Req, P> From<P> for UuidService<B, E, Req, P> {
    #[inline]
    fn from(parser: P) -> Self {
        Self {
            backends: Distance::new(),
            parser,
            cfg: Default::default(),
            _mark: Default::default(),
        }
    }
}

impl<B, E, Req, P> Hash for UuidService<B, E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
    B: Send + Sync,
{
    #[inline]
    fn hash<K: HashKey>(&self, _k: &K) -> i64 {
        0
    }
}

impl<B, E, Req, P> Topology for UuidService<B, E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
    B: Send + Sync,
{
}

impl<B: Send + Sync, E, Req, P> Endpoint for UuidService<B, E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
{
    type Item = Req;
    #[inline]
    fn send(&self, mut req: Self::Item) {
        log::debug!("+++ {} send => {:?}", self.cfg.service, req);

        if *req.context_mut() == 0 {
            if let Some(quota) = self.backends.quota() {
                req.quota(quota);
            }
        }

        let ctx = super::transmute(req.context_mut());
        let (idx, endpoint) = if ctx.runs == 0 {
            self.backends.unsafe_select()
        } else {
            unsafe {
                self.backends
                    .unsafe_next(ctx.idx as usize, ctx.runs as usize)
            }
        };
        ctx.idx = idx as u16;
        ctx.runs += 1;

        let try_next = ctx.runs == 1;
        req.try_next(try_next);
        endpoint.1.send(req);
    }

    #[inline]
    fn shard_idx(&self, _hash: i64) -> usize {
        0
    }
}
impl<B, E, Req, P> TopologyWrite for UuidService<B, E, Req, P>
where
    B: Builder<P, Req, E>,
    P: Protocol,
    E: Endpoint<Item = Req> + Single,
{
    #[inline]
    fn update(&mut self, namespace: &str, cfg: &str) {
        if let Some(ns) = UuidNamespace::try_from(cfg) {
            self.cfg.update(namespace, ns);
        }
    }
    #[inline]
    fn need_load(&self) -> bool {
        self.cfg.need_load()
    }

    #[inline]
    fn load(&mut self) {
        self.cfg.load_guard().check_load(|| self.load_inner());
    }
}
impl<B, E, Req, P> discovery::Inited for UuidService<B, E, Req, P>
where
    E: discovery::Inited,
{
    #[inline]
    fn inited(&self) -> bool {
        self.backends.len() > 0
            && self
                .backends
                .iter()
                .fold(true, |inited, (_, e)| inited && e.inited())
    }
}

impl<B, E, Req, P> UuidService<B, E, Req, P>
where
    B: Builder<P, Req, E>,
    P: Protocol,
    E: Endpoint<Item = Req> + Single,
{
    #[inline]
    fn take_or_build(&self, old: &mut HashMap<String, Vec<E>>, addr: &str, timeout: Timeout) -> E {
        let service = &self.cfg.service;
        match old.get_mut(addr).map(|endpoints| endpoints.pop()) {
            Some(Some(end)) => end,
            _ => B::build(&addr, self.parser.clone(), Resource::Uuid, service, timeout),
        }
    }

    #[inline]
    fn load_inner(&mut self) -> bool {
        let mut addrs = Vec::new();
        for shard in self.cfg.shards_url.iter() {
            for url_port in shard {
                let url = url_port.host();
                let port = url_port.port();
                use ds::vec::Add;
                let mut notlookup = false;
                dns::lookup_ips(url, |ips| {
                    for ip in ips {
                        addrs.add(ip.to_string() + ":" + port);
                    }
                    if ips.len() == 0 {
                        notlookup = true;
                    }
                });
                if notlookup {
                    log::warn!("addr {url} not looked up");
                    return false;
                }
            }
        }

        // 到这之后，所有的shard都能解析出ip
        let mut old = HashMap::with_capacity(self.backends.len());
        for backend in self.backends.take() {
            old.entry(backend.0).or_insert(Vec::new()).push(backend.1);
        }
        let mut backends = Vec::with_capacity(addrs.len());
        for addr in addrs {
            assert_ne!(addr.len(), 0);
            let backend = self.take_or_build(&mut old, &addr, self.cfg.timeout());
            // backend.enable_single();
            backends.push((addr, backend));
        }
        use crate::PerformanceTuning;
        self.backends = Distance::with_performance_tuning(
            backends,
            self.cfg.basic.selector.tuning_mode(),
            self.cfg.basic.region_enabled,
        );

        log::info!("{} load complete. dropping:{:?}", self.cfg.service, {
            old.retain(|_k, v| v.len() > 0);
            old.keys()
        });

        true
    }
}

impl<B: Send + Sync, E, Req, P> std::fmt::Display for UuidService<B, E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UuidService")
            .field("cfg", &self.cfg)
            .field("backends", &self.backends.len())
            .finish()
    }
}
