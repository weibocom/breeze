extern crate json;

use std::{
    collections::HashMap,
    io::{Error, ErrorKind},
};

use crate::memcache::MemcacheConf;
use serde::{Deserialize, Serialize};
use serde_yaml::Mapping;
use url::Url;

pub struct Vintage {
    base_url: Url,
    biz_mc_confs: HashMap<String, MemcacheConf>,
}

#[derive(Serialize, Deserialize, Debug)]
struct ConfigResponse {
    code: String,
    body: MemcacheRspConf,
}

/**
 * memcache conf
 **/
#[derive(Serialize, Deserialize, Debug)]
struct MemcacheRspConf {
    #[serde(rename = "groupId")]
    group_id: String,
    sign: String,
    nodes: Vec<MemcacheRspConfNode>,
}

/**
 * memcache conf中的node
 **/
#[derive(Serialize, Deserialize, Debug)]
pub struct MemcacheRspConfNode {
    key: String,
    value: String,
}

impl Vintage {
    pub fn from_url(_url: Url) -> Self {
        Self {
            base_url: _url,
            biz_mc_confs: HashMap::new(),
        }
    }

    //TODO: 根据业务的namespace获取mc conf，然后进行hash、layer、路由选择
    pub fn get_service(&self, _name: &str) -> String {
        // 两组l1, 一主一从
        //"127.0.0.1:11211;127.0.0.1:11212,127.0.0.1:11213;127.0.0.1:11214;127.0.0.1:11215;"

        "127.0.0.1:11211".to_owned()
    }

    /**
     * @return 返回HashMap<group, MemcacheConf>
     */
    pub async fn lookup(
        &self,
        group: &str,
        key: &str,
    ) -> Result<HashMap<String, MemcacheConf>, Box<dyn std::error::Error>> {
        // 设置config的path
        let mut gurl = self.base_url.clone();
        gurl.set_path("1/config/service");

        // http 请求并进行json解析
        let resp = reqwest::Client::new()
            .get(gurl)
            .query(&[("action", "lookup"), ("group", group), ("key", key)])
            .send()
            .await?
            .json::<ConfigResponse>()
            .await?;

        if "200".ne(resp.code.as_str()) {
            return Err(Box::new(Error::new(
                ErrorKind::NotFound,
                "not found conf when lookup",
            )));
        }

        // ignore emtpy result
        if resp.body.nodes.len() < 1 {
            return Err(Box::new(Error::new(
                ErrorKind::InvalidData,
                "nodes is empty",
            )));
        }

        // parse body with yaml format
        let conf_str = &resp.body.nodes[0].value;
        let conf_mapping: Mapping = serde_yaml::from_str(conf_str)?;
        let mut biz_confs: HashMap<String, MemcacheConf> = HashMap::new();
        for (k, v) in conf_mapping {
            if "global".to_string() == k {
                continue;
            }
            let temp_cnf: MemcacheConf = serde_yaml::from_value(v)?;
            biz_confs.insert(k.as_str().unwrap().to_string(), temp_cnf);
        }

        println!("lookup group/{} key/{}, rs: {:?}", group, key, biz_confs);

        Ok(biz_confs)
    }
}

#[cfg(test)]
mod mc_discovery_test {
    use super::Vintage;
    use url::Url;

    #[test]
    fn test_lookup() {
        let vintage_base = Url::parse("http://127.0.0.1/");
        let vintage = Vintage::from_url(vintage_base.unwrap());
        let conf_task = vintage.lookup("cache.service2.0.unread.pool.lru.test", "all");

        let rt = tokio::runtime::Runtime::new().unwrap();
        let conf = rt.block_on(conf_task);
        println!("lookup result: {:?}", conf);
        assert!(conf.unwrap().len() > 0);
    }
}
