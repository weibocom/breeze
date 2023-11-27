use crate::{Backend, Endpoint};
use sharding::distribution::Distribute;

#[derive(Clone)]
pub(crate) struct Shards<E, Req> {
    router: Distribute,
    backends: Vec<(E, String)>,
    _mark: std::marker::PhantomData<Req>,
}
impl<E, Req> Endpoint for Shards<E, Req>
where
    E: Endpoint<Item = Req>,
    Req: protocol::Request,
{
    type Item = Req;
    #[inline]
    fn send(&self, req: Req) {
        let idx = self.shard_idx(req.hash());
        unsafe {
            assert!(idx < self.backends.len());
            self.backends.get_unchecked(idx).0.send(req);
        }
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
}
impl<E, Req> Backend for Shards<E, Req>
where
    E: Endpoint<Item = Req>,
    Req: protocol::Request,
{
    fn available(&self) -> bool {
        true
    }
}

impl<E, Req> discovery::Inited for Shards<E, Req>
where
    E: discovery::Inited,
{
    fn inited(&self) -> bool {
        self.backends.len() > 0
            && self
                .backends
                .iter()
                .fold(true, |inited, e| inited && e.0.inited())
    }
}

impl<E, Req> Into<Vec<(E, String)>> for Shards<E, Req> {
    #[inline]
    fn into(self) -> Vec<(E, String)> {
        self.backends
    }
}

impl<E, Req> Shards<E, Req> {
    #[inline]
    pub fn from<B: FnMut(&str) -> E>(dist: &str, addrs: Vec<String>, mut builder: B) -> Self {
        let router = Distribute::from(dist, &addrs);
        let backends = addrs
            .into_iter()
            .map(|addr| (builder(&addr), addr))
            .collect();
        Self {
            router,
            backends,
            _mark: Default::default(),
        }
    }
}

use discovery::distance::Addr;
impl<E, Req> Addr for Shards<E, Req> {
    #[inline]
    fn addr(&self) -> &str {
        self.backends.get(0).map(|b| b.1.as_str()).unwrap_or("")
    }
    fn visit(&self, f: &mut dyn FnMut(&str)) {
        self.backends.iter().for_each(|b| f(b.1.as_str()))
    }
}
