use discovery::{Inited, TopologyWrite};
use protocol::{Protocol, Request, ResOption, Resource};
use sharding::hash::{Hash, HashKey};

use crate::Timeout;

pub type TopologyProtocol<E, R, P> = Topologies<E, R, P>;

// 1. 生成一个try_from(parser, endpoint)的方法，endpoint是名字的第一个单词或者是所有单词的首字母。RedisService的名字为"rs"或者"redis"
// 2. trait => where表示，为Topologies实现trait，满足where的条件.
//    第一个参数必须是self，否则无法dispatcher
// 3. 如果trait是pub的，则同时会创建这个trait。非pub的trait，只会为Topologies实现
procs::topology_dispatcher! {
    #[derive(Clone)]
    pub enum Topologies<E, R, P> {
        MsgQue(crate::msgque::topo::MsgQue<E, R, P>),
        RedisService(crate::redisservice::topo::RedisService<E, R, P>),
        CacheService(crate::cacheservice::topo::CacheService<E, R, P>),
        PhantomService(crate::phantomservice::topo::PhantomService<E, R, P>),
        KvService(crate::kv::topo::KvService<E, R, P>),
        UuidService(crate::uuid::topo::UuidService<E, R, P>),
    }

    pub trait Endpoint: Sized + Send + Sync {
        type Item;
        fn send(&self, req: Self::Item);
        fn shard_idx(&self, _hash: i64) -> usize {todo!("shard_idx not implemented");}
        fn available(&self) -> bool {todo!("available not implemented");}
        fn addr(&self) -> &str {"addr not implemented"}
        fn build_o<P:Protocol>(_addr: &str, _p: P, _r: Resource, _service: &str, _to: Timeout, _o: ResOption) -> Self {todo!("build not implemented")}
        fn build<P:Protocol>(addr: &str, p: P, r: Resource, service: &str, to: Timeout) -> Self {Self::build_o(addr, p, r, service, to, Default::default())}
    } => where P:Protocol, E:Endpoint<Item = R> + Inited, R: Request

    pub trait Topology : Endpoint + Hash{
        fn exp_sec(&self) -> u32 {86400}
    } => where P:Protocol, E:Endpoint<Item = R>, R:Request, Topologies<E, R, P>: Endpoint

    trait Inited {
        fn inited(&self) -> bool;
    } => where E:Inited

    trait TopologyWrite {
        fn update(&mut self, name: &str, cfg: &str);
        fn disgroup<'a>(&self, _path: &'a str, cfg: &'a str) -> Vec<(&'a str, &'a str)>;
        fn need_load(&self) -> bool;
        fn load(&mut self) -> bool;
    } => where P:Protocol, E:Endpoint<Item = R>

    trait Hash {
        fn hash<S: HashKey>(&self, key: &S) -> i64;
    } => where P:Protocol, E:Endpoint<Item = R>, R:Request

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

pub struct Pair<E> {
    pub addr: String,
    pub endpoint: E,
}
impl<E: Endpoint> From<(String, E)> for Pair<E> {
    fn from(pair: (String, E)) -> Pair<E> {
        Pair {
            addr: pair.0,
            endpoint: pair.1,
        }
    }
}
impl<E: Endpoint> From<E> for Pair<E> {
    fn from(pair: E) -> Pair<E> {
        Pair {
            addr: pair.addr().to_string(),
            endpoint: pair,
        }
    }
}

use std::collections::HashMap;
pub struct Endpoints<'a, R, P, E: Endpoint> {
    service: &'a str,
    parser: &'a P,
    resource: Resource,
    cache: HashMap<String, Vec<E>>,
    _marker: std::marker::PhantomData<R>,
}
impl<'a, R, P, E: Endpoint> Endpoints<'a, R, P, E> {
    pub fn new(service: &'a str, parser: &'a P, resource: Resource) -> Self {
        Endpoints {
            service,
            parser,
            resource,
            cache: HashMap::new(),
            _marker: Default::default(),
        }
    }
    pub fn cache_one<T: Into<Pair<E>>>(&mut self, endpoint: T) {
        self.cache(vec![endpoint]);
    }
    pub fn cache<T: Into<Pair<E>>>(&mut self, endpoints: Vec<T>) {
        self.cache.reserve(endpoints.len());
        for pair in endpoints.into_iter().map(|e| e.into()) {
            self.cache
                .entry(pair.addr)
                .or_insert(Vec::new())
                .push(pair.endpoint);
        }
    }
    pub fn with_cache<T: Into<Pair<E>>>(mut self, endpoints: Vec<T>) -> Self {
        self.cache(endpoints);
        self
    }
}

impl<'a, R, P: Protocol, E: Endpoint> Endpoints<'a, R, P, E> {
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
impl<'a, R, P, E: Endpoint> std::fmt::Display for Endpoints<'a, R, P, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut exists = Vec::new();
        for (_addr, endpoints) in self.cache.iter() {
            for e in endpoints {
                exists.push(e.addr());
            }
        }
        write!(
            f,
            "service:{} resource:{} addrs:{:?}",
            self.service,
            self.resource.name(),
            exists
        )
    }
}

// 为Endpoints实现Drop
impl<'a, R, P, E: Endpoint> Drop for Endpoints<'a, R, P, E> {
    fn drop(&mut self) {
        log::info!("drop endpoints:{}", self);
    }
}
