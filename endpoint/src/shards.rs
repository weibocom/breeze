use crate::Endpoint;
use sharding::distribution::Distribute;

#[derive(Clone)]
pub(crate) struct Shards<E> {
    router: Distribute,
    backends: Vec<E>,
}
impl<E, Req> Endpoint for Shards<E>
where
    E: Endpoint<Item = Req>,
    Req: protocol::Request,
{
    type Item = Req;
    #[inline]
    fn send(&self, req: Req) {
        let idx = self.shard_idx(req.hash());
        assert!(idx < self.backends.len());
        unsafe { self.backends.get_unchecked(idx).send(req) };
    }

    #[inline]
    fn shard_idx(&self, hash: i64) -> usize {
        assert!(self.backends.len() > 0);
        if self.backends.len() > 1 {
            self.router.index(hash)
        } else {
            0
        }
    }
    #[inline(always)]
    fn available(&self) -> bool {
        true
    }
}

use discovery::Inited;
impl<E: Inited> Inited for Shards<E> {
    fn inited(&self) -> bool {
        self.backends.len() > 0
            && self
                .backends
                .iter()
                .fold(true, |inited, e| inited && e.inited())
    }
}

impl<E: Endpoint> Into<Vec<E>> for Shards<E> {
    #[inline]
    fn into(self) -> Vec<E> {
        self.backends
    }
}

impl<E: Endpoint> Shards<E> {
    pub fn from_dist(dist: &str, group: Vec<E>) -> Self {
        let addrs = group.iter().map(|e| e.addr()).collect::<Vec<_>>();
        let router = Distribute::from(dist, &addrs);
        Self {
            router,
            backends: group,
        }
    }
}

use discovery::distance::Addr;
impl<E: Endpoint> Addr for Shards<E> {
    #[inline]
    fn addr(&self) -> &str {
        self.backends.get(0).map(|b| b.addr()).unwrap_or("")
    }
    fn visit(&self, f: &mut dyn FnMut(&str)) {
        self.backends.iter().for_each(|b| f(b.addr()))
    }
}

use crate::select::Distance;
#[derive(Clone)]
pub struct Shard<E> {
    pub(crate) master: E,
    pub(crate) slaves: Distance<E>,
}
impl<E: Endpoint> Shard<E> {
    #[inline]
    pub fn selector(performance: bool, master: E, replicas: Vec<E>, region_enabled: bool) -> Self {
        Self {
            master,
            slaves: Distance::with_mode(replicas, performance, region_enabled),
        }
    }
}
impl<E> Shard<E> {
    #[inline]
    pub(crate) fn has_slave(&self) -> bool {
        self.slaves.len() > 0
    }
    #[inline]
    pub(crate) fn master(&self) -> &E {
        &self.master
    }
    #[inline]
    pub(crate) fn select(&self) -> (usize, &E) {
        self.slaves.unsafe_select()
    }
    #[inline]
    pub(crate) fn next(&self, idx: usize, runs: usize) -> (usize, &E)
    where
        E: Endpoint,
    {
        unsafe { self.slaves.unsafe_next(idx, runs) }
    }
    pub(crate) fn check_region_len(&self, ty: &str, service: &str)
    where
        E: Endpoint,
    {
        use discovery::dns::IPPort;
        let addr = self.master.addr();
        let port = addr.port();
        let len_region = self.slaves.len_region();

        // 开启可用区且可用区内资源数量为0，才生成监控数据；
        // 暂不考虑从开启可用区变更到不开启场景
        if len_region == Some(0) {
            let f = |r: &str| format!("{}:{}", r, port);
            let region_port = context::get()
                .region()
                .map(f)
                .unwrap_or_else(|| f(discovery::distance::host_region().as_str()));

            // 构建metric数据
            let path = metrics::Path::new(vec![ty, service, &*region_port]);
            let mut metric = path.status("region_resource");
            metric += metrics::Status::NOTIFY; // 生成可用区内资源实例数量不足的监控数据
        }
    }
}
impl<E: discovery::Inited> Shard<E> {
    // 1. 主已经初始化
    // 2. 有从
    // 3. 所有的从已经初始化
    #[inline]
    pub(crate) fn inited(&self) -> bool {
        self.master().inited()
            && self.has_slave()
            && self
                .slaves
                .iter()
                .fold(true, |inited, e| inited && e.inited())
    }
}
// 为Shard实现Debug
impl<E: Endpoint> std::fmt::Debug for Shard<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shard")
            .field("master", &self.master.addr())
            .field("slaves", &self.slaves)
            .finish()
    }
}
