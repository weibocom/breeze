use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use discovery::dns;
use discovery::dns::IPPort;
use discovery::TopologyWrite;
use ds::time::Duration;
use protocol::Protocol;
use protocol::Request;
use protocol::ResOption;
use protocol::Resource;
use sharding::distribution::Distribute;
use sharding::hash::Hasher;
use sharding::ReplicaSelect;
use sharding::Selector;

use crate::Builder;
use crate::Single;
use crate::TimeoutAdjust;
use crate::{Endpoint, Topology};

use super::config::MysqlNamespace;
const CONFIG_UPDATED_KEY: &str = "__mysql_config__";

#[derive(Clone)]
pub struct MysqlService<B, E, Req, P> {
    // 默认后端分片，一共shards.len()个分片，每个分片 shard[0]是master, shard[1..]是slave
    direct_shards: Vec<Shard<E>>,
    // 默认不同sharding的url。第0个是master
    direct_shards_url: Vec<Vec<String>>,

    // todo: 暂不实现这块处理逻辑
    // 按时间维度分库分表
    archive_shards: HashMap<String, Vec<Shard<E>>>,
    archive_shards_url: HashMap<String, Vec<Vec<String>>>,

    hasher: Hasher,
    distribute: Distribute,
    selector: Selector, // 从的选择策略。
    updated: HashMap<String, Arc<AtomicBool>>,
    parser: P,
    service: String,
    timeout_master: Duration,
    timeout_slave: Duration,
    _mark: std::marker::PhantomData<(B, Req)>,
    user: String,
    password: String,
}

impl<B, E, Req, P> From<P> for MysqlService<B, E, Req, P> {
    #[inline]
    fn from(parser: P) -> Self {
        Self {
            parser,
            direct_shards: Default::default(),
            direct_shards_url: Default::default(),
            archive_shards: Default::default(),
            archive_shards_url: Default::default(),
            hasher: Default::default(),
            distribute: Default::default(),
            updated: Default::default(),
            service: Default::default(),
            selector: Selector::Random,
            timeout_master: crate::TO_MYSQL_M,
            timeout_slave: crate::TO_MYSQL_S,
            _mark: Default::default(),
            user: Default::default(),
            password: Default::default(),
        }
    }
}

impl<B, E, Req, P> Topology for MysqlService<B, E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
    B: Send + Sync,
{
    fn hasher(&self) -> &sharding::hash::Hasher {
        &self.hasher
    }
}

impl<B: Send + Sync, E, Req, P> Endpoint for MysqlService<B, E, Req, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
{
    type Item = Req;

    // todo: 这里req拿到mid解出时间,得出具体年库 ？？？
    // todo: sql语句怎么传过去 ？
    fn send(&self, req: Self::Item) {
        debug_assert_ne!(self.direct_shards.len(), 0);
        let shard_idx = self.distribute.index(req.hash());
        debug_assert!(
            shard_idx < self.direct_shards.len(),
            "mysql: {}/{} req:{:?}",
            shard_idx,
            self.direct_shards.len(),
            req
        );
        let shard = unsafe { self.direct_shards.get_unchecked(shard_idx) };
        log::debug!("+++ {} send {} => {:?}", self.service, shard_idx, req);
        // 如果有从，并且是读请求，如果目标server异常，会重试其他slave节点
        if shard.has_slave() && !req.operation().is_store() {
            //todo: 访问slave
            shard.master().send(req);
        } else {
            shard.master().send(req);
        }
    }

    fn shard_idx(&self, hash: i64) -> usize {
        self.distribute.index(hash)
    }
}

impl<B, E, Req, P> TopologyWrite for MysqlService<B, E, Req, P>
where
    B: Builder<P, Req, E>,
    P: Protocol,
    E: Endpoint<Item = Req> + Single,
{
    fn need_load(&self) -> bool {
        self.direct_shards.len() != self.direct_shards_url.len()
            || self
                .updated
                .iter()
                .fold(false, |acc, (_k, v)| acc || v.load(Ordering::Acquire))
    }
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
                .expect("mysql config state missed")
                .store(true, Ordering::Release);
            log::warn!("mysql will reload topo later...");
        }
    }
    fn update(&mut self, namespace: &str, cfg: &str) {
        if let Some(ns) = MysqlNamespace::try_from(cfg) {
            self.timeout_master.adjust(ns.basic.timeout_ms_master);
            self.timeout_slave.adjust(ns.basic.timeout_ms_slave);
            self.hasher = Hasher::from(&ns.basic.hash);
            self.distribute = Distribute::from(ns.basic.distribution.as_str(), &ns.backends);
            self.selector = ns.basic.selector.as_str().into();
            self.user = ns.basic.user.as_str().into();
            self.password = ns.basic.password.as_str().into();

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
            if self.direct_shards_url.len() > 0 {
                log::debug!(
                    "top updated from {:?} to {:?}",
                    self.direct_shards_url,
                    shards_url
                );
            }
            self.direct_shards_url = shards_url;

            //todo: archive shard 未处理

            // 配置更新完毕，如果watcher确认配置update了，各个topo就重新进行load
            self.updated
                .entry(CONFIG_UPDATED_KEY.to_string())
                .or_insert(Arc::new(AtomicBool::new(true)))
                .store(true, Ordering::Release);
        }
        self.service = namespace.to_string();
    }
}
impl<B, E, Req, P> MysqlService<B, E, Req, P>
where
    B: Builder<P, Req, E>,
    P: Protocol,
    E: Endpoint<Item = Req> + Single,
{
    // todo: mysql tcp connection and msql handshake complete
    // 这里需要把用户名/密码 都传过去 ？？
    // #[inline]
    fn take_or_build(
        &self,
        old: &mut HashMap<String, Vec<E>>,
        addr: &str,
        timeout: Duration,
        res: ResOption,
    ) -> E {
        match old.get_mut(addr).map(|endpoints| endpoints.pop()) {
            Some(Some(end)) => end,
            _ => B::build(
                &addr,
                self.parser.clone(),
                Resource::Mysql,
                &self.service,
                timeout,
                res,
            ),
        }
    }
    #[inline]
    fn load_inner(&mut self) -> bool {
        // 所有的ip要都能解析出主从域名
        let mut addrs = Vec::with_capacity(self.direct_shards_url.len());
        for shard in self.direct_shards_url.iter() {
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

        let mut old = HashMap::with_capacity(self.direct_shards_url.len());
        for shard in self.direct_shards.split_off(0) {
            old.entry(shard.master.0)
                .or_insert(Vec::new())
                .push(shard.master.1);
            for (addr, endpoint) in shard.slaves.into_inner() {
                // 一个ip可能存在于多个域名中。
                old.entry(addr).or_insert(Vec::new()).push(endpoint);
            }
        }
        // 用户名和密码
        let mut res_option = ResOption::default();
        res_option.token = self.password.clone();
        res_option.username = self.user.clone();

        // 遍历所有的shards_url
        for (master_addr, slaves) in addrs {
            assert_ne!(master_addr.len(), 0);
            assert_ne!(slaves.len(), 0);
            let master = self.take_or_build(
                &mut old,
                &master_addr,
                self.timeout_master,
                res_option.clone(),
            );
            master.enable_single();

            // slave
            let mut replicas = Vec::with_capacity(8);
            for addr in slaves {
                let slave =
                    self.take_or_build(&mut old, &addr, self.timeout_slave, res_option.clone());
                slave.disable_single();
                replicas.push((addr, slave));
            }
            let shard = Shard::selector(self.selector, master_addr, master, replicas);
            self.direct_shards.push(shard);
        }
        assert_eq!(
            self.direct_shards.len(),
            self.direct_shards_url.len(),
            "shards/urs: {}/{}",
            self.direct_shards.len(),
            self.direct_shards_url.len()
        );
        log::info!(
            "{} load complete. {} dropping:{:?}",
            self.service,
            self.direct_shards.len(),
            {
                old.retain(|_k, v| v.len() > 0);
                old.keys()
            }
        );

        true
    }
}
impl<B, E, Req, P> discovery::Inited for MysqlService<B, E, Req, P>
where
    E: discovery::Inited,
{
    // 每一个域名都有对应的endpoint，并且都初始化完成。
    #[inline]
    fn inited(&self) -> bool {
        // direct_shards 实例初始化
        self.direct_shards.len() > 0
            && self.direct_shards.len() == self.direct_shards_url.len()
            && self
                .direct_shards
                .iter()
                .fold(true, |inited, direct_shards| {
                    inited && direct_shards.inited()
                })

        // todo: archive_shards 实例没有初始化完成
    }
}

// todo: 这一段跟redis是一样的，这段可以提到外面去
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
// todo: 这一段跟redis是一样的，这段可以提到外面去
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

// todo: 这一段跟redis是一样的，这段可以提到外面去
#[derive(Clone)]
struct Shard<E> {
    master: (String, E),
    slaves: ReplicaSelect<(String, E)>,
}
