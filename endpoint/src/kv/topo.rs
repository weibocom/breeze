use std::collections::HashMap;

use discovery::dns;
use discovery::dns::IPPort;
use discovery::TopologyWrite;
use ds::MemGuard;
use protocol::kv::Binary;
use protocol::kv::ContextStatus;
use protocol::kv::MysqlBuilder;
use protocol::kv::Strategy;
use protocol::Protocol;
use protocol::Request;
use protocol::ResOption;
use protocol::Resource;
use rand::seq::SliceRandom;
use sharding::hash::{Hash, HashKey};

use crate::dns::DnsConfig;
use crate::Timeout;
use crate::{shards::Shard, Endpoint, Topology};

use super::config::KvNamespace;
use super::config::Years;
use super::strategy::Strategist;
use super::KVCtx;
#[derive(Clone)]
pub struct KvService<E, Req, P> {
    shards: Shards<E>,
    // selector: Selector,
    strategist: Strategist,
    parser: P,
    cfg: Box<DnsConfig<KvNamespace>>,
    _mark: std::marker::PhantomData<Req>,
}

impl<E, Req, P> From<P> for KvService<E, Req, P> {
    #[inline]
    fn from(parser: P) -> Self {
        Self {
            parser,
            shards: Default::default(),
            strategist: Default::default(),
            cfg: Default::default(),
            _mark: std::marker::PhantomData,
            // selector: Selector::Random,
        }
    }
}

impl<E, Req, P> Hash for KvService<E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
{
    #[inline]
    fn hash<K: HashKey>(&self, k: &K) -> i64 {
        self.strategist.hasher().hash(k)
    }
}

impl<E, Req, P> Topology for KvService<E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
{
}

impl<E, Req, P> Endpoint for KvService<E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
{
    type Item = Req;

    fn send(&self, mut req: Self::Item) {
        // req 是mc binary协议，需要展出字段，转换成sql
        let (intyear, shard_idx) = if req.ctx_mut().runs == 0 {
            let key = req.key();
            //定位年库
            let intyear: u16 = self.strategist.get_key(&key);
            let shard_idx = self.shard_idx(req.hash());
            req.ctx_mut().year = intyear;
            req.ctx_mut().shard_idx = shard_idx as u16;

            //todo: 此处不应panic
            let cmd =
                MysqlBuilder::build_packets(&self.strategist, &req, &key).expect("malformed sql");
            req.reshape(MemGuard::from_vec(cmd));

            (intyear, shard_idx)
        } else {
            (req.ctx_mut().year, req.ctx_mut().shard_idx as usize)
        };

        let shards = self.shards.get(intyear);
        if shards.len() == 0 {
            req.ctx_mut().error = ContextStatus::TopInvalid;
            req.on_err(protocol::Error::TopInvalid);
            return;
        }
        debug_assert!(
            shard_idx < shards.len(),
            "mysql: {}/{} req:{:?}",
            shard_idx,
            shards.len(),
            req
        );

        let shard = unsafe { shards.get_unchecked(shard_idx) };
        log::debug!(
            "mysql {:?} send {} year {} shards {:?} => {:?}",
            self.cfg,
            shard_idx,
            intyear,
            shards,
            req
        );

        if shard.has_slave() && !req.operation().is_store() {
            if *req.context_mut() == 0 {
                if let Some(quota) = shard.slaves.quota() {
                    req.quota(quota);
                }
            }
            let ctx = req.ctx_mut();
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
            // 只重试一次，重试次数过多，可能会导致雪崩。如果不重试，现在的配额策略在当前副本也只会连续发送四次请求，问题也不大
            let try_next = ctx.runs == 1;
            req.try_next(try_next);
            endpoint.send(req)
        } else {
            shard.master().send(req);
        }
    }

    fn shard_idx(&self, hash: i64) -> usize {
        self.strategist.distribution().index(hash)
    }
}

impl<E, Req, P> TopologyWrite for KvService<E, Req, P>
where
    P: Protocol,
    E: Endpoint<Item = Req>,
{
    fn need_load(&self) -> bool {
        self.shards.len() != self.cfg.shards_url.len() || self.cfg.need_load()
    }
    fn load(&mut self) -> bool {
        self.cfg.load_guard().check_load(|| self.load_inner())
    }
    fn update(&mut self, namespace: &str, cfg: &str) {
        if let Some(ns) = KvNamespace::try_from(cfg) {
            self.strategist = Strategist::try_from(&ns);
            self.cfg.update(namespace, ns);
        }
    }
}
impl<E, Req, P> KvService<E, Req, P>
where
    P: Protocol,
    E: Endpoint<Item = Req>,
{
    // #[inline]
    fn take_or_build(
        &self,
        old: &mut HashMap<String, Vec<E>>,
        addr: &str,
        timeout: Timeout,
        res: ResOption,
    ) -> E {
        match old.get_mut(addr).map(|endpoints| endpoints.pop()) {
            Some(Some(end)) => end,
            _ => E::build_o(
                &addr,
                self.parser.clone(),
                Resource::Mysql,
                &self.cfg.service,
                timeout,
                res,
            ),
        }
    }
    #[inline]
    fn load_inner(&mut self) -> bool {
        // 所有的ip要都能解析出主从域名
        let mut addrs = Vec::with_capacity(self.cfg.backends.len());
        for (interval, shard) in &self.cfg.backends {
            let mut addrs_per_interval = Vec::with_capacity(shard.len());
            for shard in shard.iter() {
                let shard: Vec<&str> = shard.split(",").collect();
                if shard.len() < 2 {
                    log::warn!("{} both master and slave required.", self.cfg.service);
                    return false;
                }
                let master_url = &shard[0];
                let mut master = String::new();
                dns::lookup_ips(master_url.host(), |ips| {
                    if ips.len() > 0 {
                        master = ips[0].to_string() + ":" + master_url.port();
                    }
                });
                let mut slaves = Vec::with_capacity(8);
                for url_port in &shard[1..] {
                    let url = url_port.host();
                    let port = url_port.port();
                    use ds::vec::Add;
                    dns::lookup_ips(url, |ips| {
                        for ip in ips {
                            slaves.add(ip.to_string() + ":" + port);
                        }
                    });
                }
                if master.len() == 0 || slaves.len() == 0 {
                    log::warn!(
                        "master:({}=>{}) or slave ({:?}=>{:?}) not looked up",
                        master_url,
                        master,
                        &shard[1..],
                        slaves
                    );
                    return false;
                }
                addrs_per_interval.push((master, slaves));
            }
            addrs.push((interval, addrs_per_interval));
        }

        // 到这之后，所有的shard都能解析出ip
        let mut old = HashMap::with_capacity(self.shards.len());
        for shard in self.shards.take() {
            old.entry(shard.master.addr().to_string())
                .or_insert(Vec::new())
                .push(shard.master);
            for endpoint in shard.slaves.into_inner() {
                let addr = endpoint.addr().to_string();
                // 一个ip可能存在于多个域名中。
                old.entry(addr).or_insert(Vec::new()).push(endpoint);
            }
        }

        let mut rng = rand::thread_rng();
        for (interval, addrs_per_interval) in addrs {
            let mut shards_per_interval = Vec::with_capacity(addrs_per_interval.len());
            // 遍历所有的shards_url
            for (master_addr, mut slaves) in addrs_per_interval {
                assert_ne!(master_addr.len(), 0);
                assert_ne!(slaves.len(), 0);
                // 用户名和密码
                let res_option = ResOption {
                    token: self.cfg.basic.password.clone(),
                    username: self.cfg.basic.user.clone(),
                };
                let master = self.take_or_build(
                    &mut old,
                    &master_addr,
                    self.cfg.timeout_master(),
                    res_option.clone(),
                );
                // slave
                let mut replicas = Vec::with_capacity(8);
                if self.cfg.basic.max_slave_conns != 0 {
                    slaves.shuffle(&mut rng);
                    slaves.truncate(self.cfg.basic.max_slave_conns as usize);
                }
                for addr in slaves {
                    let slave = self.take_or_build(
                        &mut old,
                        &addr,
                        self.cfg.timeout_slave(),
                        res_option.clone(),
                    );
                    replicas.push(slave);
                }

                use crate::PerformanceTuning;
                let shard = Shard::selector(
                    self.cfg.basic.selector.tuning_mode(),
                    master,
                    replicas,
                    false,
                );
                shards_per_interval.push(shard);
            }
            self.shards.push((interval, shards_per_interval));
        }
        assert_eq!(self.shards.len(), self.cfg.shards_url.len());
        log::info!("{} load complete. dropping:{:?}", self.cfg.service, {
            old.retain(|_k, v| v.len() > 0);
            old.keys()
        });
        true
    }
}
impl<E, Req, P> discovery::Inited for KvService<E, Req, P>
where
    E: discovery::Inited,
{
    // 每一个域名都有对应的endpoint，并且都初始化完成。
    #[inline]
    fn inited(&self) -> bool {
        // 每一个分片都有初始, 并且至少有一主一从。
        self.shards.len() > 0
            && self.shards.len() == self.cfg.shards_url.len()
            && self.shards.inited()
    }
}

// todo: 这一段跟redis是一样的，这段可以提到外面去
//impl<E: discovery::Inited> Shard<E> {
//    // 1. 主已经初始化
//    // 2. 有从
//    // 3. 所有的从已经初始化
//    #[inline]
//    fn inited(&self) -> bool {
//        self.master().inited()
//            && self.has_slave()
//            && self
//                .slaves
//                .iter()
//                .fold(true, |inited, e| inited && e.inited())
//    }
//}
// todo: 这一段跟redis是一样的，这段可以提到外面去
//impl<E: Endpoint> Shard<E> {
//    #[inline]
//    fn selector(is_performance: bool, master: E, replicas: Vec<E>, region_enabled: bool) -> Self {
//        Self {
//            master,
//            slaves: Distance::with_mode(replicas, is_performance, region_enabled),
//        }
//    }
//}
//impl<E> Shard<E> {
//    #[inline]
//    fn has_slave(&self) -> bool {
//        self.slaves.len() > 0
//    }
//    #[inline]
//    fn master(&self) -> &E {
//        &self.master
//    }
//    #[inline]
//    fn select(&self) -> (usize, &E) {
//        self.slaves.unsafe_select()
//    }
//    #[inline]
//    fn next(&self, idx: usize, runs: usize) -> (usize, &E)
//    where
//        E: Endpoint,
//    {
//        unsafe { self.slaves.unsafe_next(idx, runs) }
//    }
//}

// todo: 这一段跟redis是一样的，这段可以提到外面去
//#[derive(Clone)]
//struct Shard<E> {
//    master: E,
//    slaves: Distance<E>,
//}

//impl<E> Debug for Shard<E> {
//    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//        write!(f, "shard(master => slaves: {:?})", self.slaves.len())
//    }
//}

const YEAR_START: u16 = 2000;
const YEAR_END: u16 = 2099;
const YEAR_LEN: usize = (YEAR_END - YEAR_START) as usize + 1;
#[derive(Clone)]
struct Shards<E> {
    shards: Vec<Vec<Shard<E>>>,
    //2000~2099年的分片索引范围，如index[0] = 2 表示2000年的shards为shards[2]
    //使用usize::MAX表示未初始化
    index: [usize; YEAR_LEN],
    len: usize,
}

impl<E> Default for Shards<E> {
    fn default() -> Self {
        Self {
            shards: Default::default(),
            index: [usize::MAX; YEAR_LEN],
            len: 0,
        }
    }
}
impl<E> Shards<E> {
    fn len(&self) -> usize {
        self.len
    }
    //会重新初始化
    fn take(&mut self) -> Vec<Shard<E>> {
        self.index = [usize::MAX; YEAR_LEN];
        self.len = 0;
        self.shards.split_off(0).into_iter().flatten().collect()
    }
    //push 进来的shard是否init了
    fn inited(&self) -> bool
    where
        E: discovery::Inited,
    {
        self.shards
            .iter()
            .flatten()
            .fold(true, |inited, shard| inited && shard.inited())
    }

    fn year_index(year: u16) -> usize {
        (year - YEAR_START) as usize
    }

    fn push(&mut self, shards_per_interval: (&Years, Vec<Shard<E>>)) {
        let (interval, shards_per_interval) = shards_per_interval;
        let index = self.shards.len();
        self.len += shards_per_interval.len();
        self.shards.push(shards_per_interval);
        let (start_year, end_year) = (Self::year_index(interval.0), Self::year_index(interval.1));
        for i in &mut self.index[start_year..=end_year] {
            assert_eq!(*i, usize::MAX);
            *i = index
        }
    }

    fn get(&self, intyear: u16) -> &[Shard<E>] {
        if intyear > YEAR_END || intyear < YEAR_START {
            return &[];
        }
        let index = self.index[Self::year_index(intyear)];
        if index == usize::MAX {
            return &[];
        }
        &self.shards[index]
    }
}
