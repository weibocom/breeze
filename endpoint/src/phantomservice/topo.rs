use std::{
    collections::HashMap,
    marker::PhantomData,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use discovery::{
    dns::{self, IPPort},
    TopologyWrite,
};
use protocol::{Builder, Endpoint, Protocol, Request, Resource, Topology, Utf8};
use sharding::hash::Hasher;
use stream::Shards;

use super::config::{Backend, PhantomNamespace};
use super::config::{ACCESS_NONE, ACCESS_READ, ACCESS_WRITE};

#[derive(Clone)]
pub struct PhantomService<B, E, Req, P> {
    // 一般有2组，相互做HA，每组是一个域名列表，域名下只有一个ip，但会变化
    streams: Vec<(Shards<E, Req>, AccessMod)>,
    // 不同streams的url
    streams_backend: Vec<Backend>,
    updated: HashMap<String, Arc<AtomicBool>>,
    hasher: Hasher,
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
            timeout: Duration::from_millis(200),
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

impl<B, E, Req, P> protocol::Endpoint for PhantomService<B, E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
    B: Send + Sync,
{
    type Item = Req;
    #[inline]
    fn send(&self, mut req: Self::Item) {
        assert_ne!(self.streams.len(), 0);

        let mut context = super::Context::from(*req.context_mut());

        let (idx, try_next) = self.get_context(&mut context, req.operation().is_store());
        if idx >= self.streams.len() {
            log::debug!(
                "+++ ignore req for idx/{} is bigger than streams.len/{}, req: {:?}",
                idx,
                self.streams.len(),
                req.data().utf8(),
            );
            return;
        }

        // TODO 测试用例检查context里的数据变化
        req.try_next(try_next);
        *req.mut_context() = context.ctx;

        log::debug!("++++ send req with server-idx: {}", idx);
        unsafe { self.streams.get_unchecked(idx).0.send(req) };
    }
}

impl<B, E, Req, P> PhantomService<B, E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
    B: Send + Sync,
{
    //
    #[inline]
    fn get_context(&self, ctx: &mut super::Context, is_write: bool) -> (usize, bool) {
        ctx.check_and_inited(is_write);

        // TODO: 这里处理idx，随机是不是更佳？线上是按顺序访问，调通后考虑修改 fishermen
        let mut idx = ctx.take_proc_idx() as usize;
        if idx >= self.streams.len() {
            return (idx, false);
        }
        let stream_mod = &self.streams.get(idx).unwrap().1;
        while (ctx.is_write() && !stream_mod.can_write())
            || (!ctx.is_write() && !stream_mod.can_read())
        {
            idx = ctx.take_proc_idx() as usize;
            if idx >= self.streams.len() {
                return (idx, false);
            }
        }

        let try_next = idx + 1 < self.streams.len();
        (idx, try_next)
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
            self.timeout = ns.timeout();
            self.hasher = Hasher::from(&ns.basic.hash);
            self.service = namespace.to_string();

            for b in ns.backends.iter() {
                for hp in b.servers.iter() {
                    let host = hp.host();
                    if !self.updated.contains_key(host) {
                        let watcher = dns::register(host);
                        self.updated.insert(host.to_string(), watcher);
                    }
                }
            }

            if ns.backends.len() > 0 {
                log::info!(
                    "phantom/{} topo updated from {:?} to {:?}",
                    namespace,
                    self.streams_backend,
                    ns.backends
                );
            }
            self.streams_backend = ns.backends.clone();
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
        // 把通知放在最前面，避免丢失通知
        for (_, updated) in self.updated.iter() {
            updated.store(false, Ordering::Release);
        }

        let mut addrs = Vec::with_capacity(self.streams_backend.len());
        for b in self.streams_backend.iter() {
            let mut stream = Vec::with_capacity(b.servers.len());
            for hp in b.servers.iter() {
                let host_url = hp.host();
                let ips = dns::lookup_ips(host_url);
                if ips.len() == 0 {
                    log::warn!("phantom dns looked up failed for {}", hp);
                    return;
                }
                if ips.len() > 1 {
                    log::warn!(
                        "host/{} has {} ips: {:?}, will use{}",
                        host_url,
                        ips.len(),
                        ips,
                        ips[0]
                    );
                }
                let addr = ips[0].clone() + ":" + host_url.port();
                stream.push(addr);
            }
            addrs.push(stream);
        }

        let old_streams = self.streams.split_off(0);
        self.streams.reserve(old_streams.len());
        let mut old = HashMap::with_capacity(old_streams.len() * 8);

        for shard in old_streams {
            let pool: Vec<(E, String)> = shard.0.into();
            for e in pool {
                old.insert(e.1, e.0);
            }
        }

        for b in self.streams_backend.clone() {
            let access_mod = AccessMod::from(b.access_mod.as_str());
            let shard = self.build(&mut old, b, self.service.as_str(), self.timeout);
            self.streams.push((shard, access_mod));
        }
    }
}

impl<B, E, Req, P> PhantomService<B, E, Req, P>
where
    B: Builder<P, Req, E>,
    P: Protocol,
    E: Endpoint<Item = Req>,
{
    fn build(
        &self,
        old: &mut HashMap<String, E>,
        backend: Backend,
        name: &str,
        timeout: Duration,
    ) -> Shards<E, Req> {
        Shards::from(backend.distribution.as_str(), backend.servers, |addr| {
            old.remove(addr).map(|e| e).unwrap_or_else(|| {
                B::build(addr, self.parser.clone(), Resource::Phantom, name, timeout)
            })
        })
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
            && self
                .streams
                .iter()
                .fold(true, |inited, (s, _)| inited && s.inited())
    }
}

#[derive(Clone)]
struct AccessMod {
    read: bool,
    write: bool,
}

impl AccessMod {
    #[inline]
    fn from(access_mod: &str) -> Self {
        let access = access_mod.to_ascii_lowercase();
        // access mod 只有 none、r、w、rw四种组合
        if ACCESS_NONE.eq(&access) {
            return AccessMod {
                read: false,
                write: false,
            };
        }
        assert!(access.len() <= 2);
        let rmod = access.contains(ACCESS_READ);
        let wmod = access.contains(ACCESS_WRITE);
        Self {
            read: rmod,
            write: wmod,
        }
    }

    #[inline]
    fn can_read(&self) -> bool {
        self.read
    }

    #[inline]
    fn can_write(&self) -> bool {
        self.write
    }
}
