pub(crate) mod cache;
pub(crate) mod cfg;
pub mod distance;
pub mod socks;

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

use enum_dispatch::enum_dispatch;

#[derive(Debug)]
pub enum Config<C> {
    NotFound,
    NotChanged,
    Config(String, C), // 第一个元素是签名，第二个是数据
}

#[enum_dispatch]
pub trait Discover {
    ///name 格式为domain/path/to/path
    fn get_service<C>(
        &self,
        name: &str,
        sig: &str,
    ) -> impl std::future::Future<Output = Result<Config<C>>> + Send
    where
        C: Unpin + Send + From<String>;
}

#[enum_dispatch(Discover)]
pub enum Discovery {
    Vintage(Vintage),
}
impl Discovery {
    pub fn from_url(url: &Url) -> Self {
        let schem = url.scheme();
        // let http = Self::copy_url_to_http(&url);
        match schem {
            "vintage" => Self::Vintage(Vintage::default()),
            _ => panic!("not supported endpoint name"),
        }
    }
}

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
