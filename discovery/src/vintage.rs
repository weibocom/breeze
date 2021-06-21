extern crate json;

use std::{
    collections::{HashMap, HashSet},
    io::{Error, ErrorKind},
};

use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::time;

// use crate::memcache::MemcacheConf;
use serde::{Deserialize, Serialize};
use serde_yaml::Mapping;
use url::Url;

// common consts
const CONF_COMMON_KEY: &str = "all";
const EMPTY_STR: &str = "";

#[derive(Clone)]
pub struct Vintage {
    base_url: Url,
    biz_mc_confs: HashMap<String, String>,
    // 注册的grouop列表，一个group下一般有多个biz/namespace
    groups: HashSet<String>,
    //sender: Sender<String>,
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
    pub fn from_url(url: Url) -> Self {
        //let (tx, rx) = mpsc::channel(200);
        let vintage = Vintage {
            base_url: url,
            biz_mc_confs: HashMap::new(),
            groups: HashSet::new(),
            //sender: tx,
        };

        //vintage.start_deamon_worker(rx).await;
        //TODO： start deamon worker here

        return vintage;
    }

    async fn start_deamon_worker(&mut self, mut recv: Receiver<String>) {
        let mut this = self.clone();
        tokio::task::spawn(async move {
            loop {
                // 每个loop sleep 2-5 秒
                let sleep_mills: u64 = 2000 + rand::random::<u64>() % 3000;
                time::sleep(time::Duration::from_millis(sleep_mills)).await;

                // 一旦从channel有group，连续处理所有的滞留的group
                loop {
                    match recv.recv().await {
                        Some(group) => match this.lookup(group.as_str()).await {
                            Ok(rs) => {
                                println!("lookup success for {}, count: {}", group, rs.len());
                            }
                            Err(e) => {
                                println!("found err when lookup {}, err: {:?}", group, e);
                            }
                        },
                        None => {
                            println!("recv none in vintage, so will stop and wait");
                            break;
                        }
                    }
                }
            }
        });
    }

    //TODO: 根据业务的namespace获取mc conf，然后进行hash、layer、路由选择
    pub fn get_service(&self, _name: &str) -> String {
        // 两组l1, 一主一从
        //"127.0.0.1:11211;127.0.0.1:11212,127.0.0.1:11213;127.0.0.1:11214;127.0.0.1:11215;"
        // "127.0.0.1:11211".to_owned()
        let default = EMPTY_STR.to_string();
        let topo_conf = self.biz_mc_confs.get(_name.into()).unwrap_or(&default);
        return topo_conf.to_string();
    }

    // pub fn parse_mc_conf(conf: &String) -> MemcacheConf {
    //     let mc: MemcacheConf = serde_yaml::from_str(conf.as_str()).unwrap();
    //     return mc;
    // }

    /**
     * req url: curl "http://XXX/1/config/service?action=lookup&group=test"
     * {"code":"200","body":{"groupId":"xx.test","sign":"88c4b30f63cc4ff3ba2bded7fef85fff","nodes":[{"key":"all","value":"global:\n  lru_max_memory: 2000000000\n  get_sign_time: 10000\n  heart_beat_time: 2000\n\nuser:\n  hash: bkdr\n  distribution: ketama\n  hash_tag: user\n  save_tag: false\n  auto_eject_hosts: false\n  timeout: 300\n  lru_timeout: 100000\n  redis: false\n  partial_reply: true\n  server_retry_timeout: 10000\n  server_failure_limit: 2\n  exptime: 1987200000\n  #master_l1:\n  #- \n  # - 10.13.4.140:10001\n  master:\n   - 10.13.4.140:10002\n  slave:\n   - 10.13.4.140:10001\n\n\n"}]}}
     * @return 返回HashMap<group, MemcacheConf>
     */
    pub async fn lookup(
        &mut self,
        group: &str,
    ) -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
        // 设置config的path
        let mut gurl = self.base_url.clone();
        gurl.set_path("1/config/service");

        // http 请求并进行json解析
        let resp = reqwest::Client::new()
            .get(gurl)
            .query(&[
                ("action", "lookup"),
                ("group", group),
                ("key", CONF_COMMON_KEY),
            ])
            .send()
            .await?
            .json::<ConfigResponse>()
            .await?;

        if "200".ne(resp.code.as_str()) {
            return Err(Box::new(Error::new(
                ErrorKind::NotFound,
                format!("lookup failed. code is not 200:{}", resp.code),
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
        println!("config str:{}", conf_str);
        let conf_mapping: Mapping = serde_yaml::from_str(conf_str)?;
        let mut biz_confs: HashMap<String, String> = HashMap::new();
        for (k, v) in conf_mapping {
            if "global".to_string() == k {
                continue;
            }
            //let temp_cnf: MemcacheConf = serde_yaml::from_value(v)?;
            biz_confs.insert(k.as_str().unwrap().into(), v.as_str().unwrap().into());
        }

        // update vintage cache
        for (k, v) in biz_confs.clone() {
            self.biz_mc_confs.insert(k, v);
        }

        // update groups
        self.groups.insert(group.into());

        println!(
            "lookup group/{} key/{}, rs: {:?}",
            group, CONF_COMMON_KEY, biz_confs
        );

        Ok(biz_confs)
    }

    // 订阅group，注意可以支持订阅任意个group，所有group的key均位all

    //pub async fn subscribe(&mut self, group: &str) {
    //    let rs = self.groups.insert(group.into());
    //    if !rs {
    //        println!("subscribe duplicat group: {}", group);
    //        return;
    //    }

    //    // 通过channel方式进行通知
    //    match self.sender.send(group.into()).await {
    //        Ok(_) => {
    //            println!("subscribe for group/{} sended", group)
    //        }
    //        Err(e) => println!("err: {:?}", e),
    //    }
    //}
}

#[cfg(test)]
mod mc_discovery_test {
    use super::Vintage;
    use url::Url;

    #[test]
    fn test_lookup() {
        let rt = tokio::runtime::Runtime::new().unwrap();

        let vintage_base = Url::parse("http://config.vintage:80/");

        let mut vintage = Vintage::from_url(vintage_base.unwrap());
        let conf_task = vintage.lookup("cache.service2.0.unread.pool.lru.test");
        let conf = rt.block_on(conf_task);

        println!("lookup result: {:?}", conf);
        assert!(conf.unwrap().len() > 0);
    }
}

use async_trait::async_trait;

#[async_trait]
impl super::Discover for Vintage {
    async fn get_service(
        &self,
        name: &str,
        sig: &str,
    ) -> std::io::Result<Option<(String, String)>> {
        Ok(None)
        // 从name中解析出group和namespace
        //let group_ns: Vec<&str> = name.split("#").collect();
        //if group_ns.len() != 2 {
        //    println!("malformed param/{} when get conf", name);
        //    return Err(Error::new(
        //        ErrorKind::InvalidInput,
        //        "malformed param in get_service",
        //    ));
        //}

        //// 根据group查询分组配置，根据namespace获取具体子业务配置
        //let group = *group_ns.get(0).unwrap();
        //let namespace = *group_ns.get(1).unwrap();
        //let mut this = self.clone();
        //match this.lookup(group).await {
        //    Ok(confs) => match confs.get(namespace) {
        //        Some(c) => {
        //            return Ok(Some((namespace.to_string(), c.to_string())));
        //        }
        //        None => {
        //            println!("not found namespace in group confs:{}", name);
        //            return Err(Error::new(ErrorKind::InvalidInput, "not found namespace"));
        //        }
        //    },
        //    Err(e) => {
        //        return Err(Error::new(
        //            ErrorKind::InvalidInput,
        //            format!("lookup error:{:?}", e),
        //        ));
        //    }
        //};
    }
}
