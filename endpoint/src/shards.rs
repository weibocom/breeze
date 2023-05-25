use crate::Endpoint;
use protocol::request::Request;
use sharding::distribution::Distribute;

#[derive(Clone)]
pub(crate) struct Shards<E> {
    router: Distribute,
    backends: Vec<(E, String)>,
}
impl<E> Endpoint for Shards<E>
where
    E: Endpoint,
{
    #[inline]
    fn send(&self, req: Request) {
        // assert!(self.backends.len() > 0);
        // let idx = if self.backends.len() > 1 {
        //     self.router.index(req.hash())
        // } else {
        //     0
        // };
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

impl<E> discovery::Inited for Shards<E>
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

impl<E> Into<Vec<(E, String)>> for Shards<E> {
    #[inline]
    fn into(self) -> Vec<(E, String)> {
        self.backends
    }
}

impl<E> Shards<E> {
    #[inline]
    pub fn from<B: FnMut(&str) -> E>(dist: &str, addrs: Vec<String>, mut builder: B) -> Self {
        let router = Distribute::from(dist, &addrs);
        let backends = addrs
            .into_iter()
            .map(|addr| (builder(&addr), addr))
            .collect();
        Self { router, backends }
    }
}

use discovery::distance::Addr;
impl<E> Addr for Shards<E> {
    #[inline]
    fn addr(&self) -> &str {
        self.backends.get(0).map(|b| b.1.as_str()).unwrap_or("")
    }
    fn visit(&self, f: &mut dyn FnMut(&str)) {
        self.backends.iter().for_each(|b| f(b.1.as_str()))
    }
}
