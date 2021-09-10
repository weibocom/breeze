extern crate json;

use std::io::{Error, ErrorKind};

use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Clone)]
pub struct Vintage {
    base_url: Url,
}

#[derive(Serialize, Deserialize, Debug)]
struct Node {
    index: String,
    data: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct Response {
    message: String,
    node: Node,
}

impl From<Response> for (String, String) {
    fn from(resp: Response) -> Self {
        (resp.node.data, resp.node.index)
    }
}

impl Vintage {
    pub fn from_url(url: Url) -> Self {
        Self { base_url: url }
    }

    async fn lookup<C>(&self, path: &str, index: &str) -> std::io::Result<Config<C>>
    where
        C: From<String>,
    {
        // 下来这段逻辑用于测试。把vintage先临时禁用，验证对cpu的影响
        if index.len() > 4 {
            log::info!("vintage lookup cached for {:?} index:{}", path, index);
            return Ok(Config::NotChanged);
        }
        // 设置config的path
        let mut gurl = self.base_url.clone();
        gurl.set_path(path);

        log::info!("vintage-lookup: path:{} index:{}", path, index);

        let resp = reqwest::Client::new()
            .get(gurl)
            .query(&[("index", index)])
            .send()
            .await
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
        match resp.status().as_u16() {
            // not modified
            304 => Ok(Config::NotChanged),
            404 => Ok(Config::NotFound),
            200 => {
                let resp: Response = resp
                    .json()
                    .await
                    .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
                if resp.message != "ok" {
                    Err(Error::new(ErrorKind::Other, resp.message))
                } else {
                    let (data, index) = resp.into();
                    Ok(Config::Config(C::from(data), index))
                }
            }
            status => {
                let msg = format!("{} not a valid vintage status.", status);
                Err(Error::new(ErrorKind::Other, msg))
            }
        }
    }
}

use super::{Config, ServiceId};
use async_trait::async_trait;

#[async_trait]
impl super::Discover for Vintage {
    #[inline]
    async fn get_service<S, C>(&self, name: S, sig: &str) -> std::io::Result<Config<C>>
    where
        S: Unpin + Send + ServiceId,
        C: Unpin + Send + From<String>,
    {
        let path = name.path();
        self.lookup(path, sig).await
    }
}
