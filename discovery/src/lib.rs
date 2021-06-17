pub mod cache;
mod memcache;

// mod empty_vintage;
// pub use empty_vintage::Vintage;
mod update;
mod vintage;
use vintage::Vintage;

use url::Url;

pub trait Discover {
    fn get_service(&self, name: &str) -> String;
}

pub trait ServiceDiscover {
    type Item;
    fn get(&self) -> &Self::Item;
}

pub enum Discovery {
    Vintage(Vintage),
}

impl Discovery {
    pub async fn from_url(url: Url, groups: Vec<&str>) -> Self {
        match url.scheme() {
            "vintage" => {
                let mut vt = Vintage::from_url(url).await;
                for g in groups {
                    vt.subscribe(g).await;
                }
                Self::Vintage(vt)
            }
            _ => panic!("not a vintage schema"),
        }
    }
}

impl Discover for Discovery {
    #[inline]
    fn get_service(&self, name: &str) -> String {
        match self {
            Discovery::Vintage(v) => v.get_service(name),
        }
    }
}

pub trait Topology: Default {
    fn copy_from(&self, cfg: &str) -> Self;
}

pub struct ServiceDiscovery<D, T> {
    cache: T,
    discovery: D,
    service: String,
}

impl<D, T> ServiceDiscovery<D, T> {
    pub fn new(discovery: D, service: String) -> Arc<Self>
    where
        D: Discover,
        T: Topology,
    {
        let cfg = discovery.get_service(&service);
        let cache = T::default().copy_from(&cfg);
        let me = Self {
            discovery,
            service,
            cache: cache,
        };
        let me = Arc::new(me);
        me.clone().start_watch();
        me
    }
    fn start_watch(self: Arc<Self>) {}
}

impl<D: Discover, T> ServiceDiscover for ServiceDiscovery<D, T> {
    type Item = T;
    #[inline]
    fn get(&self) -> &Self::Item {
        &self.cache
    }
}

use std::sync::Arc;
impl<T: Discover> Discover for Arc<T> {
    #[inline]
    fn get_service(&self, name: &str) -> String {
        (**self).get_service(name)
    }
}
impl<T: ServiceDiscover> ServiceDiscover for Arc<T> {
    type Item = T::Item;
    #[inline]
    fn get(&self) -> &T::Item {
        (**self).get()
    }
}
