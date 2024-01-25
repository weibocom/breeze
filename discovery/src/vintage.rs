//extern crate json;

use std::io::{Error, ErrorKind::Other};

use ds::time::{timeout, Duration};
use hyper::{client::HttpConnector, Client, Uri};
use serde::Deserialize;

pub struct Vintage {
    client: Client<HttpConnector>,
}

#[derive(Deserialize)]
struct Node {
    index: String,
    data: String,
}

#[derive(Deserialize)]
struct Response {
    message: String,
    node: Node,
}

impl Response {
    fn into(self) -> (String, String) {
        (self.node.index, self.node.data)
    }
}

impl Default for Vintage {
    fn default() -> Self {
        //多个域名也会连接复用
        Vintage {
            client: Client::new(),
        }
    }
}

impl Vintage {
    // pub fn from_url(url: Url) -> Self {
    //     Self {
    //         base_url: url,
    //         client: Client::new(),
    //     }
    // }
    //fn get_url(&self, host_path: &str) -> Url {
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
    //Url::parse(&format!("http://{host_path}")).unwrap()
    //}

    async fn lookup<C>(
        &self,
        path: &str,
        index: &str,
    ) -> Result<Config<C>, Box<dyn std::error::Error>>
    where
        C: From<String>,
    {
        // 设置config的path
        //let gurl = self.get_url(path);
        let uri: Uri = format!("http://{path}?index={index}").parse()?;
        log::debug!("lookup: {}", uri);

        let resp = timeout(Duration::from_secs(3), self.client.get(uri)).await??;
        let status = resp.status().as_u16();
        match status {
            404 => Ok(Config::NotFound),
            304 => Ok(Config::NotChanged),
            200 => {
                let b = hyper::body::to_bytes(resp.into_body()).await?;
                let resp: Response = serde_json::from_slice(&b)?;
                if resp.message == "ok" {
                    let (t_index, data) = resp.into();
                    if t_index == index {
                        return Ok(Config::NotChanged);
                    } else {
                        log::info!("{} '{}' => '{}' len:{}", path, index, t_index, data.len());
                        return Ok(Config::Config(t_index, C::from(data)));
                    };
                } else {
                    return Err(Box::new(Error::new(Other, resp.message)));
                }
            }
            status => Err(Box::new(Error::new(Other, status.to_string()))),
        }
    }
}

use super::Config;

impl super::Discover for Vintage {
    #[inline]
    async fn get_service<C>(&self, name: &str, sig: &str) -> std::io::Result<Config<C>>
    where
        C: Unpin + Send + From<String>,
    {
        self.lookup(name, sig)
            .await
            .map_err(|e| Error::new(Other, e.to_string()))
    }
}
