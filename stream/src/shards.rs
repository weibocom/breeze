use protocol::Endpoint;
use sharding::distribution::Distribute;

#[derive(Clone)]
pub struct Shards<E, Req> {
    router: Distribute,
    backends: Vec<(E, String)>,
    _mark: std::marker::PhantomData<Req>,
}
impl<E, Req> protocol::Endpoint for Shards<E, Req>
where
    E: Endpoint<Item = Req>,
    Req: protocol::Request,
{
    type Item = Req;
    #[inline(always)]
    fn send(&self, req: Req) {
        debug_assert!(self.backends.len() > 0);
        let idx = if self.backends.len() > 1 {
            self.router.index(req.hash())
        } else {
            0
        };
        unsafe {
            self.backends.get_unchecked(idx).0.send(req);
        }
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
