use crate::select::Distance;
use crate::{Endpoint, Endpoints, Topology};
use discovery::TopologyWrite;
use protocol::memcache::Binary;
use protocol::{Protocol, Request, Resource::Memcache};
use sharding::hash::{Hash, HashKey, Hasher};

use super::config::Flag;
use crate::shards::Shards;
use crate::PerformanceTuning;
use protocol::Bit;

#[derive(Clone)]
pub struct CacheService<E, Req, P> {
    // 一共有n组，每组1个连接。
    // 排列顺序： master, master l1, slave, slave l1
    streams: Distance<Shards<E>>,
    // streams里面的前r_num个数据是提供读的(这个长度不包含slave l1, slave)。
    hasher: Hasher,
    parser: P,
    exp_sec: u32,

    // TODO 线上稳定后再清理，预计2024.2之后
    // 1. 去掉force_write_all，其设计的本意是set失败后，是否更新其他layer；
    // 当前的设计原则已经改为可用性优先，只要有layer可用，就应该对外提供服务，所以force_write_all都应该为true，也就失去了存在的价值了；
    //
    // 兼容已有业务逻辑，set master失败后，是否更新其他layer
    // force_write_all: bool,

    // 保留本设置，非必要场景，减少一次slave访问
    backend_no_storage: bool, // true：mc后面没有存储
    _marker: std::marker::PhantomData<Req>,
}

impl<E, Req, P> From<P> for CacheService<E, Req, P> {
    #[inline]
    fn from(parser: P) -> Self {
        Self {
            parser,
            streams: Distance::new(),
            exp_sec: 0,
            // force_write_all: false, // 兼容考虑默认为false，set master失败后，不更新其他layers，新业务推荐用true
            hasher: Default::default(),
            _marker: Default::default(),
            backend_no_storage: false,
        }
    }
}

impl<E, Req, P> discovery::Inited for CacheService<E, Req, P>
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

impl<E, Req, P> Hash for CacheService<E, Req, P>
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

impl<E, Req, P> Topology for CacheService<E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
{
    #[inline]
    fn exp_sec(&self) -> u32 {
        self.exp_sec
    }
}

impl<E, Req, P> Endpoint for CacheService<E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
{
    type Item = Req;
    #[inline]
    fn send(&self, mut req: Self::Item) {
        debug_assert!(self.streams.local_len() > 0);

        // let mut idx: usize = 0; // master
        let mut ctx = super::Context::from(*req.mut_context());
        // gets及store类指令，都需要先请求master，然后再考虑masterL1
        let (idx, try_next, write_back) = if req.operation().is_store() {
            self.context_store(&mut ctx)
        } else {
            if !ctx.inited() {
                // ctx未初始化, 是第一次读请求；仅第一次请求记录时间，原因如下：
                // 第一次读一般访问L1，miss之后再读master；
                // 读quota的更新根据第一次的请求时间更合理
                if let Some(quota) = self.streams.quota() {
                    req.quota(quota);
                }
            }
            self.context_get(&mut ctx, &req)
        };
        req.try_next(try_next);
        req.write_back(write_back);
        // TODO 有点怪异，先实现，晚点调整，这个属性直接从request获取更佳？ fishermen
        req.retry_on_rsp_notok(req.can_retry_on_rsp_notok());
        *req.mut_context() = ctx.ctx;
        if idx >= self.streams.len() {
            req.on_err(protocol::Error::TopChanged);
            return;
        }

        log::debug!("+++ request sent prepared:{} - {} {}", idx, req, self);
        debug_assert!(idx < self.streams.len(), "{} {} => {:?}", idx, self, req);

        unsafe { self.streams.get_unchecked(idx).send(req) };
    }
}
impl<E, Req: Request, P: Protocol> CacheService<E, Req, P>
where
    E: Endpoint<Item = Req>,
{
    #[inline]
    fn context_store(&self, ctx: &mut super::Context) -> (usize, bool, bool) {
        let (idx, try_next, write_back);
        ctx.check_and_inited(true);
        if ctx.is_write() {
            // 写指令，总是从master开始
            idx = ctx.take_write_idx() as usize;
            write_back = idx + 1 < self.streams.len();

            // topo控制try_next，只要还有layers，topo都支持try next
            try_next = idx + 1 < self.streams.len();
        } else {
            // 是读触发的回种的写请求
            idx = ctx.take_read_idx() as usize;
            write_back = false; // 只尝试回种一次。
            try_next = false; // 不再需要错误重试
        };
        (idx, try_next, write_back)
    }
    // 第一次访问到L1，下一次访问M
    // 第一次访问到M，下一次访问L1
    // 最多访问两次
    // 对于mc做存储场景，也最多访问两次
    //   若有L1，则两次访问分布在M、L1
    //   若无L1，则两次访问分布在M、S；#654
    #[inline]
    fn context_get(&self, ctx: &mut super::Context, req: &Req) -> (usize, bool, bool) {
        let (idx, try_next, write_back);
        if !ctx.check_and_inited(false) {
            // 第一个retrieve，如果需要master-first，则直接访问master
            idx = match req.operation().master_first() {
                true => 0,
                false => self.streams.select_idx(),
            };

            // 第一次访问，没有取到master，则下一次一定可以取到master
            // 如果取到了master，有slave也可以继续访问
            // 后端无storage且后端资源不止一组，可以多访问一次
            try_next = (self.streams.local_len() > 1)
                || self.backend_no_storage && (self.streams.len() > 1);
            write_back = false;
        } else {
            let last_idx = ctx.index();
            try_next = false;
            // 不是第一次访问，获取上一次访问的index
            // 上一次是主，则有从取从，上一次不是主，则取主。
            if last_idx != 0 {
                idx = 0;
            } else {
                // 满足#654场景，如果没有MasterL1，这里idx需要选到Slave
                // 同时，为对于gets成功，请求路径按照固定顺序进行  fishermen
                idx = match req.operation().master_first() {
                    true => 1,
                    false => self.streams.select_next_idx(0, 1),
                };
            }
            write_back = true;
        }
        // 把当前访问过的idx记录到ctx中，方便回写时使用。
        ctx.write_back_idx(idx as u16);
        (idx, try_next, write_back)
    }
}
impl<E, Req, P> TopologyWrite for CacheService<E, Req, P>
where
    P: Protocol,
    E: Endpoint<Item = Req>,
{
    #[inline]
    fn update(&mut self, namespace: &str, cfg: &str) {
        if let Some(ns) = super::config::Namespace::try_from(cfg, namespace) {
            self.hasher = Hasher::from(&ns.hash);

            self.exp_sec = (ns.exptime / 1000) as u32; // 转换成秒

            // self.force_write_all = ns.flag.get(Flag::ForceWriteAll as u8);
            self.backend_no_storage = ns.flag.get(Flag::BackendNoStorage as u8);
            let dist = &ns.distribution.clone();

            // 把所有的endpoints cache下来
            let mut endpoints: Endpoints<'_, Req, P, E> =
                Endpoints::new(namespace, &self.parser, Memcache);
            self.streams.take().into_iter().for_each(|shard| {
                endpoints.cache(shard.into());
            });

            let mto = crate::TO_MC_M.to(ns.timeout_ms_master);
            let rto = crate::TO_MC_S.to(ns.timeout_ms_slave);

            //use discovery::distance::{Balance, ByDistance};
            //let master = ns.master.clone();
            let is_performance = ns.flag.get(Flag::LocalAffinity as u8).tuning_mode();
            let (local_len, backends) = ns.take_backends();

            let mut new = Vec::with_capacity(backends.len());
            for (i, group) in backends.into_iter().enumerate() {
                // 第一组是master
                let to = if i == 0 { mto } else { rto };
                let backends = endpoints.take_or_build(&group, to);
                let shard = Shards::from_dist(dist, backends);

                new.push(shard);
            }
            self.streams.update(new, local_len, is_performance);
        }
        // old 会被dopped
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

use std::fmt::{self, Display, Formatter};
impl<E, Req, P> Display for CacheService<E, Req, P> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "shards:{} local-shards:{}",
            self.streams.len(),
            self.streams.local_len(),
        )
    }
}
