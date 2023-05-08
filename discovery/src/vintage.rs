extern crate json;

use std::io::{Error, ErrorKind};

use reqwest::Client;
use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Clone, Default)]
pub struct Vintage {
    client: Client,
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

impl Response {
    fn into(self) -> (String, String) {
        (self.node.index, self.node.data)
    }
}

impl Vintage {
    // pub fn from_url(url: Url) -> Self {
    //     Self {
    //         base_url: url,
    //         client: Client::new(),
    //     }
    // }
    fn get_url(&self, mut host_path: &str) -> Url {
        //idcpath会以/开头
        if host_path.starts_with('/') {
            host_path = &host_path[1..];
        }
        // (host, path) = path.split_once(delimiter).unwrap();
        // for url in &self.base_urls {
        //     if url.host_str().unwrap() == host {
        //         return url.clone().set_path(path);
        //     }
        // }

        //直接parse吧，感觉set_path并不会快
        // let base = Url::parse(&format!("http://{host_path}"));
        // self.base_urls.push(base.clone());
        // base
        Url::parse(&format!("http://{host_path}")).unwrap()
    }

    async fn lookup<C>(&self, path: &str, index: &str) -> std::io::Result<Config<C>>
    where
        C: From<String>,
    {
        // 设置config的path
        let gurl = self.get_url(path);
        log::debug!("lookup: path:{} index:{}", path, index);

        let resp = self
            .client
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
                    let (t_index, data) = resp.into();
                    if t_index == index {
                        Ok(Config::NotChanged)
                    } else {
                        log::info!("{} '{}' => '{}' len:{}", path, index, t_index, data.len());
                        Ok(Config::Config(t_index, C::from(data)))
                    }
                }
            }
            status => {
                let msg = format!("{} not a valid vintage status.", status);
                Err(Error::new(ErrorKind::Other, msg))
            }
        }
    }
}

use super::Config;
use async_trait::async_trait;

#[async_trait]
impl super::Discover for Vintage {
    #[inline]
    async fn get_service<C>(&self, name: &str, sig: &str) -> std::io::Result<Config<C>>
    where
        C: Unpin + Send + From<String>,
    {
        self.lookup(name, sig).await
    }
}
