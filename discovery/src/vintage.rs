extern crate json;

use async_recursion::async_recursion;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::io::{Error, ErrorKind};
use url::Url;
#[derive(Clone)]
pub struct Vintage {
    client: Client,
    base_url: Url,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct RNode {
    index: String,
    data: String,
    children: Vec<RChildren>,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
struct RChildren {
    name: String,
    data: String,
}
impl RChildren {
    fn rname(self) -> (String, String) {
        (self.name, self.data)
    }
}
#[derive(Serialize, Deserialize, Debug, Clone)]
struct RResponse {
    message: String,
    node: RNode,
}
impl RResponse {
    fn rd_into(self) -> (String, String, Vec<RChildren>) {
        (self.node.index, self.node.data, self.node.children)
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Response {
    message: String,
    node: Node,
}
#[derive(Serialize, Deserialize, Debug)]
struct Node {
    index: String,
    name: String,
    data: String,
}

impl Response {
    fn into(self) -> (String, String) {
        (self.node.index, self.node.data)
    }
}

impl Vintage {
    pub fn from_url(url: Url) -> Self {
        Self {
            base_url: url,
            client: Client::new(),
        }
    }

    async fn lookup<C>(&self, path: &str, index: &str) -> std::io::Result<Config<C>>
    where
        C: From<String>,
    {
        // 设置config的path
        let mut gurl = self.base_url.clone();
        gurl.set_path(path);
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
                        log::info!("{} from {} to {} len:{}", path, index, t_index, data.len());
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

    async fn finddata(&self, end_url: String, index: String) -> std::io::Result<String> {
        let resp = self
            .client
            .get(end_url)
            .query(&[("children", "true")])
            .send()
            .await
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
        let resp: Response = resp
            .json()
            .await
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
        let (t_index, data) = resp.into();
        Ok(data)
    }

    #[async_recursion]
    async fn recursion(&self, url: String, index: String) -> std::io::Result<String> {
        let mut end_url = Url::parse(&url).unwrap();
        let http_resp = self
            .client
            .get(end_url)
            .query(&[("children", "true")])
            .send()
            .await
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
        let uresp: RResponse = http_resp
            .json()
            .await
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
        let (t_index, mut data, children) = uresp.clone().rd_into();
        let mut sdata = String::new();
        for child in children.iter() {
            let end_child = child;
            let end_data = &end_child.data;
            let end_name = &end_child.name;
            let mut end_url = url.clone();
            end_url.push_str("/");
            end_url.push_str(&end_name);
            if end_data.is_empty() {
                sdata.push_str(&self.recursion(end_url, index.clone()).await?);
            } else {
                sdata.push_str("\n");
                sdata.push_str(&self.finddata(end_url, index.clone()).await?);
            }
        }
        Ok(sdata)
    }
    async fn rdlookup<C>(&self, uname: String, index: String) -> std::io::Result<Config<C>>
    where
        C: From<String>,
    {
        let mut f_url = self.base_url.clone().to_string();
        f_url.push_str(&uname);
        let aurl = Url::parse(&f_url).unwrap();

        let http_resp = self
            .client
            .get(aurl)
            .query(&[("children", "true"), ("index", &index)])
            .send()
            .await
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
        match http_resp.status().as_u16() {
            // not modified
            304 => Ok(Config::NotChanged),
            404 => Ok(Config::NotFound),
            200 => {
                let uresp: RResponse = http_resp
                    .json()
                    .await
                    .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
                let (t_index, mut data, children) = uresp.clone().rd_into();
                if uresp.message != "ok" {
                    Err(Error::new(ErrorKind::Other, uresp.message))
                } else {
                    if t_index == index {
                        Ok(Config::NotChanged)
                    } else {
                        data = self.recursion(f_url, t_index.clone()).await?;
                        log::info!(" from {} to {} len:{}", index, t_index, data.len());
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
//use protocol::Resource;
#[async_trait]
impl super::Discover for Vintage {
    #[inline]
    async fn get_service<C>(
        &self,
        name: &str,
        sig: &str,
        kindof_database: &str,
    ) -> std::io::Result<Config<C>>
    where
        C: Unpin + Send + From<String>,
    {
        match kindof_database {
            "mc" => {
                println!("mcccccccccccccc");
                self.lookup(name, sig).await
            }
            "redis" => {
                println!("redisssssssssss");
                self.rdlookup(name.to_owned(), sig.to_owned()).await
            }
            _ => {
                let msg = format!("not a valid vintage database");
                return Err(Error::new(ErrorKind::Other, msg));
            }
        }
    }
}
