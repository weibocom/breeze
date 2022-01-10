use discovery::TopologyWrite;
use protocol::{Builder, Endpoint, Protocol, Request, Resource, Topology};
use sharding::hash::Hasher;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use stream::Shards;

#[derive(Clone)]
pub struct CacheService<B, E, Req, P> {
    // 一共有n组，每组1个连接。
    // 排列顺序： master, master l1, slave, slave l1
    streams: Vec<Shards<E, Req>>,
    // streams里面的前r_num个数据是提供读的(这个长度不包含slave l1)。
    r_num: u16,
    rnd_idx: Arc<AtomicUsize>, // 读请求时，随机读取
    has_l1: bool,              // 是否包含masterl1
    has_slave: bool,
    hasher: Hasher,
    parser: P,
    _marker: std::marker::PhantomData<(B, Req)>,
}

impl<B, E, Req, P> From<P> for CacheService<B, E, Req, P> {
    #[inline]
    fn from(parser: P) -> Self {
        Self {
            parser,
            streams: Vec::new(),
            r_num: 0,
            has_l1: false,
            has_slave: false,
            hasher: Default::default(),
            rnd_idx: Default::default(),
            _marker: Default::default(),
        }
    }
}

impl<B, E, Req, P> discovery::Inited for CacheService<B, E, Req, P>
where
    E: discovery::Inited,
{
    #[inline]
    fn inited(&self) -> bool {
        self.streams.len() > 0
            && self
                .streams
                .iter()
                .fold(true, |inited, e| inited && e.inited())
    }
}

impl<B, E, Req, P> Topology for CacheService<B, E, Req, P>
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

impl<B: Send + Sync, E, Req, P> protocol::Endpoint for CacheService<B, E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
{
    type Item = Req;
    #[inline(always)]
    fn send(&self, mut req: Self::Item) {
        debug_assert!(self.r_num > 0);

        let mut idx: usize = 0; // master
        if !req.operation().master_only() {
            let mut ctx = super::Context::from(*req.mut_context());
            let (i, try_next, write_back) = if req.operation().is_store() {
                self.get_context_store(&mut ctx)
            } else {
                self.get_context_get(&mut ctx)
            };
            req.try_next(try_next);
            req.write_back(write_back);
            *req.mut_context() = ctx.ctx;
            idx = i;
            if idx >= self.streams.len() {
                req.on_err(protocol::Error::TopChanged);
                return;
            }
        }
        log::debug!("request sent prepared:{} {} {}", idx, req, self);
        unsafe { self.streams.get_unchecked(idx).send(req) };
    }
}
impl<B: Send + Sync, E, Req: Request, P: Protocol> CacheService<B, E, Req, P>
where
    E: Endpoint<Item = Req>,
{
    #[inline(always)]
    fn get_context_store(&self, ctx: &mut super::Context) -> (usize, bool, bool) {
        let (idx, try_next, write_back);
        ctx.check_and_inited(true);
        if ctx.is_write() {
            idx = ctx.take_write_idx() as usize;
            write_back = idx + 1 < self.streams.len();
            // idx == 0: 主写失败了不同步其他的从请求
            try_next = idx + 1 < self.streams.len() && idx > 0;
        } else {
            // 是读触发的回种的写请求
            idx = ctx.take_read_idx() as usize;
            write_back = false; // 只尝试回种一次。
            try_next = false; // 不再需要错误重试
        };
        (idx, try_next, write_back)
    }
    #[inline(always)]
    fn get_context_get(&self, ctx: &mut super::Context) -> (usize, bool, bool) {
        let (idx, try_next, write_back);
        if !ctx.check_and_inited(false) {
            let readable = self.r_num - self.has_slave as u16;
            debug_assert!(readable == self.r_num || readable + 1 == self.r_num);
            // 每个shard发送1024个请求再换下一个shard
            const SHIFT: u8 = 10;
            idx = (self.rnd_idx.fetch_add(1, Ordering::Relaxed) >> SHIFT) % readable as usize;
            // 第一次访问，没有取到master，则下一次一定可以取到master
            // 如果取到了master，有slave也可以继续访问
            try_next = idx != 0 || self.has_slave;
            write_back = false;
        } else {
            let last_idx = ctx.index();
            // 不是第一次访问，获取上一次访问的index
            // 上一次是主，则有从取从，上一次不是主，则取主。
            if last_idx != 0 {
                idx = 0;
                try_next = self.has_slave;
            } else {
                // 上一次取到了master，当前取slave
                idx = self.r_num as usize - 1;
                try_next = false;
            }
            write_back = true;
        }
        // 把当前访问过的idx记录到ctx中，方便回写时使用。
        ctx.write_back_idx(idx as u16);
        (idx, try_next, write_back)
    }
}
impl<B, E, Req, P> TopologyWrite for CacheService<B, E, Req, P>
where
    B: Builder<P, Req, E>,
    P: Protocol,
    E: Endpoint<Item = Req>,
{
    #[inline]
    fn update(&mut self, namespace: &str, cfg: &str) {
        super::config::Namespace::parse(cfg, namespace, |ns| {
            if ns.master.len() == 0 {
                log::info!("cache service master empty. namespace:{}", namespace);
                return;
            }
            self.hasher = Hasher::from(&ns.hash);
            let dist = &ns.distribution;

            let old_streams = self.streams.split_off(0);
            self.streams.reserve(old_streams.len());
            // 把streams按address进行flatten
            let mut streams = HashMap::with_capacity(old_streams.len() * 8);
            let old = &mut streams;

            for shards in old_streams {
                let group: Vec<(E, String)> = shards.into();
                for e in group {
                    old.insert(e.1, e.0);
                }
            }
            let mto = Duration::from_millis(500);
            // 准备master
            let master = self.build(old, ns.master, dist, namespace, mto);
            self.streams.push(master);

            let rto = Duration::from_millis(300);
            // master_l1
            self.has_l1 = ns.master_l1.len() > 0;
            for l1 in ns.master_l1 {
                let g = self.build(old, l1, dist, namespace, rto);
                self.streams.push(g);
            }

            // slave
            self.has_slave = ns.slave.len() > 0;
            if ns.slave.len() > 0 {
                let s = self.build(old, ns.slave, dist, namespace, rto);
                self.streams.push(s);
            }
            self.r_num = self.streams.len() as u16;
            for sl1 in ns.slave_l1 {
                let g = self.build(old, sl1, dist, namespace, rto);
                self.streams.push(g);
            }
            // old 会被dopped
        });
    }
    // 不同的业务共用一个配置。把不同的业务配置给拆分开
    #[inline]
    fn disgroup<'a>(&self, _path: &'a str, cfg: &'a str) -> Vec<(&'a str, &'a str)> {
        let mut v = Vec::with_capacity(16);
        use std::str;
        for item in super::config::Config::new(cfg.as_bytes()) {
            let namespace = str::from_utf8(item.0).expect("not valid utf8");
            let val = str::from_utf8(item.1).expect("not valid utf8");
            v.push((namespace, val));
        }
        v
    }
}
impl<B, E, Req, P> CacheService<B, E, Req, P>
where
    B: Builder<P, Req, E>,
    P: Protocol,
    E: Endpoint<Item = Req>,
{
    fn build(
        &self,
        old: &mut HashMap<String, E>,
        addrs: Vec<String>,
        dist: &str,
        name: &str,
        timeout: Duration,
    ) -> Shards<E, Req> {
        Shards::from(dist, addrs, |addr| {
            old.remove(addr).map(|e| e).unwrap_or_else(|| {
                B::build(addr, self.parser.clone(), Resource::Memcache, name, timeout)
            })
        })
    }
}

use std::fmt::{self, Display, Formatter};
impl<B, E, Req, P> Display for CacheService<B, E, Req, P> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "shards:{} r-shards:{} has master l1:{} has slave:{} l1 rand idx:{}",
            self.streams.len(),
            self.r_num,
            self.has_l1,
            self.has_slave,
            self.rnd_idx.load(Ordering::Relaxed),
        )
    }
}
