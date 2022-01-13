pub(crate) mod cache;
pub(crate) mod cfg;
mod topology;
mod update;
mod vintage;
pub use update::*;
pub mod dns;
mod fixed;
mod path;
mod sig;

pub use fixed::Fixed;
pub use topology::*;
use vintage::Vintage;

use std::io::Result;

use url::Url;

use async_trait::async_trait;
use enum_dispatch::enum_dispatch;

unsafe impl<C> Send for Config<C> {}
unsafe impl<C> Sync for Config<C> {}
pub enum Config<C> {
    NotFound,
    NotChanged,
    Config(String, C), // 第一个元素是签名，第二个是数据
}

#[async_trait]
#[enum_dispatch]
pub trait Discover {
    async fn get_service<C>(&self, name: &str, sig: &str) -> Result<Config<C>>
    where
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

#[async_trait]
impl<T: Discover + Send + Unpin + Sync> Discover for std::sync::Arc<T> {
    #[inline]
    async fn get_service<C>(&self, name: &str, sig: &str) -> std::io::Result<Config<C>>
    where
        C: Unpin + Send + From<String>,
    {
        (**self).get_service(name, sig).await
    }
}

pub trait ServiceId {
    fn service(&self) -> &str;
}
