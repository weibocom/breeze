mod memcache;

mod update;
mod vintage;
use update::AsyncServiceUpdate;
use vintage::Vintage;

use std::time::Duration;

use left_right::ReadHandle;

use url::Url;

pub trait Discover {
    fn get_service(&self, name: &str) -> String;
}

pub trait ServiceDiscover<T> {
    fn do_with<F, O>(&self, f: F) -> O
    where
        F: Fn(&T) -> O;
}

pub enum Discovery {
    Vintage(Vintage),
}

impl Discovery {
    pub async fn from_url(url: Url) -> Self {
        match url.scheme() {
            "vintage" => {
                let v = Vintage::from_url(url).await;
                Self::Vintage(v)
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

pub trait Topology: Default + left_right::Absorb<String> + Clone {
    fn copy_from(&self, cfg: &str) -> Self;
}

unsafe impl<T> Send for ServiceDiscovery<T> {}
unsafe impl<T> Sync for ServiceDiscovery<T> {}

pub struct ServiceDiscovery<T> {
    cache: ReadHandle<T>,
}

impl<T> ServiceDiscovery<T> {
    pub fn new<D>(discovery: D, service: String, snapshot: String, tick: Duration) -> Self
    where
        D: Discover + Send + Unpin + 'static,
        T: Topology + Send + Sync + 'static,
    {
        let (w, r) = left_right::new_from_empty::<T, String>(T::default());

        tokio::spawn(AsyncServiceUpdate::new(
            service, discovery, w, tick, snapshot,
        ));

        Self { cache: r }
    }
}

impl<T> ServiceDiscover<T> for ServiceDiscovery<T> {
    #[inline]
    fn do_with<F, O>(&self, f: F) -> O
    where
        F: Fn(&T) -> O,
    {
        f(&self.cache.enter().expect("topology not inited yes"))
    }
}

use std::sync::Arc;
impl<T: Discover> Discover for Arc<T> {
    #[inline]
    fn get_service(&self, name: &str) -> String {
        (**self).get_service(name)
    }
}
impl<T> ServiceDiscover<T> for Arc<ServiceDiscovery<T>> {
    #[inline]
    fn do_with<F, O>(&self, f: F) -> O
    where
        F: Fn(&T) -> O,
    {
        (**self).do_with(f)
    }
}
