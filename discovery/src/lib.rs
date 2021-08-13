mod name;
mod update;
mod vintage;

use name::{ServiceId, ServiceName};

use update::AsyncServiceUpdate;
use vintage::Vintage;

use std::io::Result;
use std::time::Duration;

use left_right::ReadHandle;

use url::Url;

use async_trait::async_trait;
use enum_dispatch::enum_dispatch;

unsafe impl<C> Send for Config<C> {}
unsafe impl<C> Sync for Config<C> {}
pub enum Config<C> {
    NotFound,
    NotChanged,
    Config(C, String),
}

#[async_trait]
#[enum_dispatch]
pub trait Discover {
    async fn get_service<S, C>(&self, name: S, sig: &str) -> Result<Config<C>>
    where
        S: Unpin + Send + ServiceId,
        C: Unpin + Send + From<String>;
}

#[enum_dispatch(Discover)]
pub enum Discovery {
    Vintage(Vintage),
}
impl Discovery {
    pub fn from_url(url: Url) -> Self {
        let schem = url.scheme();
        let http = Self::copy_url_to_http(&url);
        match schem {
            "vintage" => Self::Vintage(Vintage::from_url(http)),
            _ => panic!("not supported endpoint name"),
        }
    }
    fn copy_url_to_http(url: &Url) -> Url {
        let schem = url.scheme();
        let mut s = "http".to_owned();
        s.push_str(&url.as_str()[schem.len()..]);
        Url::parse(&s).unwrap()
    }
}

pub trait Topology: left_right::Absorb<(String, String)> + Clone {
    fn update(&mut self, cfg: &str, name: &str);
}

pub trait ServiceDiscover<T> {
    fn do_with<F, O>(&self, f: F) -> O
    where
        F: FnOnce(Option<&T>) -> O;
}

unsafe impl<T> Send for ServiceDiscovery<T> {}
unsafe impl<T> Sync for ServiceDiscovery<T> {}

pub struct ServiceDiscovery<T> {
    cache: ReadHandle<T>,
}

impl<T> ServiceDiscovery<T> {
    pub fn new<D>(discovery: D, service: String, snapshot: String, tick: Duration, empty: T) -> Self
    where
        D: Discover + Send + Unpin + 'static + Sync,
        T: Topology + Send + Sync + 'static,
    {
        let (w, r) = left_right::new_from_empty::<T, (String, String)>(empty);

        tokio::spawn(async move {
            AsyncServiceUpdate::new(service, discovery, w, tick, snapshot)
                .start_watch()
                .await
        });
        Self { cache: r }
    }
}

impl<T> ServiceDiscover<T> for ServiceDiscovery<T> {
    #[inline]
    fn do_with<F, O>(&self, f: F) -> O
    where
        F: FnOnce(Option<&T>) -> O,
    {
        if let Some(cache) = self.cache.enter() {
            f(Some(&cache))
        } else {
            f(None)
        }
    }
}

use std::sync::Arc;

#[async_trait]
impl<T: Discover + Send + Unpin + Sync> Discover for Arc<T> {
    #[inline]
    async fn get_service<S, C>(&self, name: S, sig: &str) -> Result<Config<C>>
    where
        S: Unpin + Send + ServiceId,
        C: Unpin + Send + From<String>,
    {
        (**self).get_service(name, sig).await
    }
}
impl<T> ServiceDiscover<T> for Arc<ServiceDiscovery<T>> {
    #[inline]
    fn do_with<F, O>(&self, f: F) -> O
    where
        F: FnOnce(Option<&T>) -> O,
    {
        (**self).do_with(f)
    }
}
