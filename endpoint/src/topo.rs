use discovery::{Inited, TopologyWrite};
use protocol::{Protocol, Request, ResOption, Resource};
use sharding::hash::{Hash, HashKey};

use crate::Timeout;

pub type TopologyProtocol<E, P> = Topologies<E, P>;

// 1. 生成一个try_from(parser, endpoint)的方法，endpoint是名字的第一个单词或者是所有单词的首字母。RedisService的名字为"rs"或者"redis"
// 2. trait => where表示，为Topologies实现trait，满足where的条件.
//    第一个参数必须是self，否则无法dispatcher
// 3. 如果trait是pub的，则同时会创建这个trait。非pub的trait，只会为Topologies实现
procs::topology_dispatcher! {
    #[derive(Clone)]
    pub enum Topologies<E, P> {
        MsgQue(crate::msgque::topo::MsgQue<E, P>),
        RedisService(crate::redisservice::topo::RedisService<E, P>),
        CacheService(crate::cacheservice::topo::CacheService<E, P>),
        PhantomService(crate::phantomservice::topo::PhantomService<E, P>),
        KvService(crate::kv::topo::KvService<E, P>),
        UuidService(crate::uuid::topo::UuidService<E, P>),
        VectorService(crate::vector::topo::VectorService<E, P>),
    }

    pub trait Endpoint: Sized + Send + Sync {
        type Item;
        fn send(&self, req: Self::Item);
        #[allow(unused_variables)]
        fn shard_idx(&self, hash: i64) -> usize {todo!("shard_idx not implemented");}
        fn available(&self) -> bool {todo!("available not implemented");}
        fn addr(&self) -> &str {"addr not implemented"}
        #[allow(unused_variables)]
        fn build_o<P:Protocol>(addr: &str, p: P, r: Resource, service: &str, to: Timeout, o: ResOption) -> Self {todo!("build not implemented")}
        fn build<P:Protocol>(addr: &str, p: P, r: Resource, service: &str, to: Timeout) -> Self {Self::build_o(addr, p, r, service, to, Default::default())}
    } => where P:Protocol, E:Endpoint<Item = R> + Inited, R: Request

    pub trait Topology : Endpoint + Hash{
        fn exp_sec(&self) -> u32 {86400}
    } => where P:Protocol, E:Endpoint<Item = R>, R:Request, Topologies<E, P>: Endpoint

    trait Inited {
        fn inited(&self) -> bool;
    } => where E:Inited

    trait TopologyWrite {
        fn update(&mut self, name: &str, cfg: &str) -> bool;
        fn disgroup<'a>(&self, _path: &'a str, cfg: &'a str) -> Vec<(&'a str, &'a str)>;
        fn need_load(&self) -> bool;
        fn load(&mut self) -> bool;
    } => where P:Protocol, E:Endpoint

    trait Hash {
        fn hash<S: HashKey>(&self, key: &S) -> i64;
    } => where P:Protocol, E:Endpoint,

}

// 从环境变量获取是否开启后端资源访问的性能模式
#[inline]
fn is_performance_tuning_from_env() -> bool {
    context::get().timeslice()
}

pub(crate) trait PerformanceTuning {
    fn tuning_mode(&self) -> bool;
}

impl PerformanceTuning for String {
    fn tuning_mode(&self) -> bool {
        is_performance_tuning_from_env()
            || match self.as_str() {
                "distance" | "timeslice" => true,
                _ => false,
            }
    }
}

impl PerformanceTuning for bool {
    fn tuning_mode(&self) -> bool {
        is_performance_tuning_from_env() || *self
    }
}

use std::collections::HashMap;
pub struct Endpoints<'a, P, E: Endpoint> {
    service: &'a str,
    parser: &'a P,
    resource: Resource,
    cache: HashMap<String, Vec<E>>,
}
impl<'a, P, E: Endpoint> Endpoints<'a, P, E> {
    pub fn new(service: &'a str, parser: &'a P, resource: Resource) -> Self {
        Endpoints {
            service,
            parser,
            resource,
            cache: HashMap::new(),
        }
    }
    pub fn cache_one(&mut self, endpoint: E) {
        self.cache(vec![endpoint]);
    }
    pub fn cache(&mut self, endpoints: Vec<E>) {
        self.cache.reserve(endpoints.len());
        for pair in endpoints.into_iter() {
            self.cache
                .entry(pair.addr().to_owned())
                .or_insert(Vec::new())
                .push(pair);
        }
    }
    pub fn with_cache(mut self, endpoints: Vec<E>) -> Self {
        self.cache(endpoints);
        self
    }
}

impl<'a, P: Protocol, E: Endpoint> Endpoints<'a, P, E> {
    pub fn take_or_build_one(&mut self, addr: &str, to: Timeout) -> E {
        self.take_or_build(&[addr.to_owned()], to)
            .pop()
            .expect("take")
    }
    pub fn take_or_build(&mut self, addrs: &[String], to: Timeout) -> Vec<E> {
        addrs
            .iter()
            .map(|addr| {
                self.cache
                    .get_mut(addr)
                    .map(|endpoints| endpoints.pop())
                    .flatten()
                    .unwrap_or_else(|| {
                        let p = self.parser.clone();
                        E::build(&addr, p, self.resource, self.service, to)
                    })
            })
            .collect()
    }
}
// 为Endpoints实现Formatter
impl<'a, P, E: Endpoint> std::fmt::Display for Endpoints<'a, P, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let addr: Vec<_> = self.cache.values().flatten().map(|e| e.addr()).collect();
        write!(f, "({} {}) => {addr:?}", self.service, self.resource.name())
    }
}

// 为Endpoints实现Drop
impl<'a, P, E: Endpoint> Drop for Endpoints<'a, P, E> {
    fn drop(&mut self) {
        log::info!("drop endpoints:{}", self);
    }
}
