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

#[derive(Serialize, Deserialize, Debug)]
struct RNode {
    index: String,
    data: String,
    children: Vec<RChildren>,
}
#[derive(Serialize, Deserialize, Debug)]
struct RChildren {
    name: String,
    data: String,
}
#[derive(Serialize, Deserialize, Debug)]
struct RResponse {
    message: String,
    node: RNode,
}
impl RResponse {
    fn rd_into(self) -> String {
        self.node.index
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
        // self.arurl::<String>(gurl.to_string());
        //println!("gurl {}", gurl.to_string());
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
                println!("resp is {:?}", resp);
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
    #[async_recursion]
    async fn arurl<C>(&self, uname: String, index: String) -> std::io::Result<Config<C>>
    where
        C: From<String>,
    {
        let mut ss = self.base_url.clone().to_string();
        ss.push_str(&uname);
        let aurl = Url::parse(&ss).unwrap();

        //println!("the begin url is {}", ss);
        let http_resp = self
            .client
            .get(aurl)
            .query(&[("children", "true")])
            .send()
            .await
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
        let resp: RResponse = http_resp
            .json()
            .await
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
        let f_index = resp.rd_into();
        println!("f_index is {}", f_index);
        // if f_index==index {
        //     Ok(Config::NotChanged)
        // }else {

        // }
        //rd_into 获取初始的index
        // for child in resp.node.children.iter() {
        //     let last_child = child;
        //     let last_data = &last_child.data;
        //     let last_name = &last_child.name;
        //     let mut recur_name = uname.clone();
        //     recur_name.push_str("/");
        //     recur_name.push_str(&last_name);
        //     if last_data.is_empty() {
        //         //println!("-------------递归---------------");
        //         self.arurl::<String>(recur_name.clone(), index.clone())
        //             .await?;
        //     } else {
        //         //println!("------------data非空-------------");
        //         let mut end_url = ss.clone();
        //         end_url.push_str("/");
        //         end_url.push_str(&last_name);
        //         println!("end url is {}", end_url);
        //         let http_resp = self
        //             .client
        //             .get(end_url)
        //             .query(&[("index", index.clone())])
        //             .send()
        //             .await
        //             .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
        //         //println!("http_resp is {:?}", http_resp);
        //         // match http_resp.status().as_u16() {
        //         //     // not modified
        //         //     304 => Ok(Config::NotChanged),
        //         //     404 => Ok(Config::NotFound),
        //         //     200 => {
        //         // let resp: Response = http_resp
        //         //     .json()
        //         //     .await
        //         //     .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
        //         // println!("resp is {:?}", resp);
        //         //         if resp.message != "ok" {
        //         //             Err(Error::new(ErrorKind::Other, resp.message));
        //         //         } else {
        //         //             let (t_index, data) = resp.into();
        //         //             if t_index == index {
        //         //                 return Ok(Config::NotChanged);
        //         //             } else {
        //         //                 log::info!(" from {} to {} len:{}", index, t_index, data.len());
        //         //                 Ok(Config::Config(&t_index, C::from(data)))
        //         //             }
        //         //         }
        //         //     }
        //         //     status => {
        //         //         let msg = format!("{} not a valid vintage status.", status);
        //         //         return Err(Error::new(ErrorKind::Other, msg));
        //         //     }
        //         // }
        //         let resp: Response = http_resp
        //             .json()
        //             .await
        //             .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
        //         // println!("resp is {:?}", resp);
        //         // let (index, t_data) = resp.into();
        //         // if &t_data == last_data {
        //         //     return Ok(Config::NotChanged);
        //         // } else {
        //         //     log::info!(
        //         //         " from {} to {} len:{}",
        //         //         //end_url.clone(),
        //         //         last_data,
        //         //         t_data,
        //         //         last_data.len()
        //         //     );
        //         //     return Ok(Config::Config(t_data, C::from(index)));
        //         // }
        //     }
        // }
        //println!("resp  hou is {:?}", resp);
        //Ok(())
        Ok(Config::NotChanged)
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
        // self.arurl(name).await
    }
    #[inline]
    async fn get_recurservice<C>(&self, uname: String, sig: String) -> std::io::Result<Config<C>>
    where
        C: Unpin + Send + From<String>,
    {
        self.arurl(uname, sig).await
    }
}
