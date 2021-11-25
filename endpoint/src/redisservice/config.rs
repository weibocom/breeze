use std::{
    collections::HashMap,
    io::{Error, ErrorKind, Result},
};

use ds::DnsResolver;
use serde::{Deserialize, Serialize};
use stream::LayerRole;

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct RedisNamespace {
    pub(crate) basic: Basic,
    pub(crate) backends: Vec<String>,
    #[serde(default)]
    pub(crate) master: Vec<String>,
    // 可能是域名，也可能是ip，调用者确认
    #[serde(default)]
    pub(crate) slaves: Vec<Vec<String>>,

    // TODO 下面这几个稍后抽取到更高一层次 fishermen
    #[serde(default)]
    pub(crate) host_addrs: HashMap<String, Vec<String>>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Basic {
    #[serde(default)]
    pub(crate) access_mod: String,
    #[serde(default)]
    pub(crate) hash: String,
    #[serde(default)]
    pub(crate) distribution: String,
    #[serde(default)]
    pub(crate) listen: String,
    #[serde(default)]
    resource_type: String,
}

// #[derive(Debug, Clone, Deserialize, Serialize)]
// pub struct Shard {
//     pub master: String,
//     pub slave: String,
// }

// impl Namespace {
//     pub fn from(basic: Basic, shards: Vec<Shard>) -> Self {
//         Self { basic, shards }
//     }
// }

impl RedisNamespace {
    pub(crate) fn parse(group_cfg: &str, namespace: &str) -> Result<RedisNamespace> {
        log::debug!("redis config/{}: {}", namespace, group_cfg);
        match serde_yaml::from_str::<RedisNamespace>(group_cfg.trim()) {
            Err(e) => {
                log::error!("parse redis/{} yml cfg failed:{:?}", namespace, group_cfg);
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("parse cfg error: {:?}", e),
                ));
            }
            Ok(rs) => {
                return Ok(rs);
            }
        }
    }

    // 包括N组slave，一组master
    pub(crate) fn readers(&self) -> Vec<(LayerRole, Vec<String>)> {
        let mut readers = Vec::with_capacity(self.slaves.len() + 1);
        for s in self.slaves.clone() {
            readers.push((LayerRole::Slave, s));
        }
        readers.push((LayerRole::Master, self.master.clone()));
        readers
    }

    async fn lookup_hosts(&mut self, resolver: &DnsResolver) -> HashMap<String, Vec<String>> {
        let mut host_addrs = HashMap::with_capacity(self.backends.len());
        for b in self.backends.clone() {
            let hosts = b.split(",");
            for h in hosts {
                let ips = resolver.lookup_ips(h).await.unwrap();
                host_addrs.insert(h.to_string(), ips);
            }
        }
        host_addrs
    }

    // 根据域名重新构建master、slave
    pub fn refresh_backends(&mut self, hosts: &HashMap<String, Vec<String>>) {
        // if self.host_addrs.eq(hosts) {
        //     log::info!("hosts not changed, so ignore refresh");
        //     return;
        // }

        self.host_addrs = hosts.clone();
        let mut master = Vec::with_capacity(self.backends.len());
        let mut slave_shards = Vec::with_capacity(self.backends.len());

        for bk in self.backends.clone() {
            let addrs: Vec<&str> = bk.split(",").collect();

            // 解析master
            let master_ips = self.host_addrs.get(addrs[0]).unwrap();
            if master_ips.len() != 1 {
                log::warn!("malformed master host: {}, ips: {:?}", addrs[0], master_ips);
                return;
            }
            debug_assert!(master_ips.len() == 1);
            master.push(master_ips[0].clone());

            // 解析slave dns
            if addrs.len() == 1 {
                continue;
            }

            let mut sshard = Vec::with_capacity(8);
            let mut idx = 1;
            while idx < addrs.len() {
                let host = addrs[idx];
                let sips = self.host_addrs.get(host).unwrap();
                if sips.len() < 1 {
                    log::warn!("malformed slave host: {}", host);
                    return;
                }
                sshard.extend(sips.clone());
                idx += 1;
            }
            slave_shards.push(sshard);
        }

        // 设置master的ip pool列表
        self.master.clear();
        for m in master {
            self.master.push(m.to_string());
        }

        // 轮询设置slave的ip pool列表
        let mut slaves = Vec::with_capacity(slave_shards.len());
        let mut shard_count = Vec::with_capacity(slave_shards.len());
        for s in slave_shards.clone() {
            debug_assert!(s.len() > 0);
            shard_count.push(s.len());
        }

        let pool_count = least_multiple_array(&shard_count);
        let mut pidx = 0;
        while pidx < pool_count {
            let mut pool = Vec::with_capacity(slave_shards.len());
            let mut sidx = 0;
            while sidx < slave_shards.len() {
                let pos = pidx % slave_shards[sidx].len();
                pool.push(slave_shards[sidx][pos].to_string());
                sidx += 1;
            }
            slaves.push(pool);
            pidx += 1;
        }
        self.slaves.clear();
        self.slaves.extend(slaves);
        log::info!("after refresh redis cfg: {:?}", self);
    }

    pub(crate) fn uniq_all(&self) -> Vec<(LayerRole, Vec<String>)> {
        let mut all = Vec::with_capacity(self.master.len() + self.slaves.len());
        all.push((LayerRole::Master, self.master.clone()));
        for s in self.slaves.clone() {
            all.push((LayerRole::Slave, s.clone()));
        }
        all
    }

    // 目前先支持dns模式，待走通后且确认ip格式后，再调整支持ips格式 fishermen
    // async fn parse_backends(&mut self) {
    //     // TODO 联调需要，暂时在每次parse时构建一个resolver，后续考虑优化（更好的用法见topology/config） fishermen
    //     let dns_resolver = DnsResolver::with_sysy_conf();
    //     let mut master: Vec<IpAddr> = Vec::with_capacity(self.backends.len());
    //     let mut slave_shards: Vec<Vec<IpAddr>> = Vec::with_capacity(self.backends.len());
    //     for bk in self.backends.clone() {
    //         let addrs: Vec<&str> = bk.split(",").collect();

    //         // 解析master
    //         let master_ips = dns_resolver.lookup_ips(addrs[0]).await;
    //         if master_ips.len() == 0 {
    //             log::warn!("parse config for redis failed: {}", addrs[0]);
    //             return;
    //         }
    //         debug_assert!(master_ips.len() == 1);
    //         master.push(master_ips[0]);

    //         // 解析slave dns
    //         if addrs.len() == 1 {
    //             continue;
    //         }
    //         debug_assert!(addrs.len() == 2);
    //         let slave_shard_ips = dns_resolver.lookup_ips(addrs[1]);
    //         if slave_shard_ips.len() == 0 {
    //             log::warn!("parse config for redis/slave failed: {}", addrs[1]);
    //             return;
    //         }
    //         slave_shards.push(slave_shard_ips);
    //     }

    //     // 设置master的ip pool列表
    //     self.master.clear();
    //     for m in master {
    //         self.master.push(m.to_string());
    //     }

    //     // 轮询设置slave的ip pool列表
    //     let mut shard_count = Vec::with_capacity(slave_shards.len());
    //     for s in slave_shards.clone() {
    //         debug_assert!(s.len() > 0);
    //         shard_count.push(s.len());
    //     }
    //     let pool_count = least_multiple_array(&shard_count);
    //     let mut pidx = 0;
    //     while pidx < pool_count {
    //         let mut pool = Vec::with_capacity(slave_shards.len());
    //         let mut sidx = 0;
    //         while sidx < slave_shards.len() {
    //             let pos = pidx % slave_shards[sidx].len();
    //             pool.push(slave_shards[sidx][pos].to_string());
    //             sidx += 1;
    //         }
    //         self.slaves.push(pool);
    //         pidx += 1;
    //     }
    // }

    pub(crate) fn parse_listen_ports(&self) -> Vec<u16> {
        if self.basic.listen.len() == 0 {
            return Vec::with_capacity(0);
        }
        let ports: Vec<u16> = self
            .basic
            .listen
            .split(",")
            .map(|p| p.parse::<u16>().unwrap_or(0))
            .filter(|p| *p > 0u16)
            .collect();
        ports
    }
}

// 求数组中的最大公约数
fn least_multiple_array(arr: &[usize]) -> usize {
    if arr.len() == 0 {
        return 1;
    }
    let mut rs = arr[0];
    for u in arr {
        rs = least_multiple(rs, *u);
    }
    rs
}

//辗转相除法求最大公约数
fn greatest_divisor(a: usize, b: usize) -> usize {
    if a % b == 0 {
        return b;
    } else {
        return greatest_divisor(b, a % b);
    }
}

//公式法求最小公倍数
fn least_multiple(a: usize, b: usize) -> usize {
    a * b / greatest_divisor(a, b)
}

#[cfg(test)]
mod config_test {

    use ds::DnsResolver;
    use serde::{Deserialize, Serialize};
    use tokio::runtime::Runtime;

    use crate::redisservice::RedisNamespace;

    #[derive(Debug, Clone, Default, Deserialize, Serialize)]
    pub struct RNT {
        pub basic: String,
    }

    // use super::RedisNamespace;

    #[test]
    fn test_parse() {
        let cfg = "redismeshtest:
          basic:
            access_mod: rw
            hash: crc32
            distribution: modula
            listen: 56810,56811,56812
            resource_type: eredis
          backends:
            - rm56810.eos.grid.sina.com.cn:56810,rs56810.hebe.grid.sina.com.cn:56810
            - rm56811.eos.grid.sina.com.cn:56811,rs56811.hebe.grid.sina.com.cn:56811
            - rm56812.eos.grid.sina.com.cn:56812,rs56812.hebe.grid.sina.com.cn:56812";

        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let mut rs = RedisNamespace::parse(cfg, "redismeshtest").unwrap();
            let resolver = DnsResolver::with_sysy_conf();
            let hosts = rs.lookup_hosts(&resolver).await;
            rs.refresh_backends(&hosts);
            print!("parse redis config:");
            println!("{}", cfg);
            // println!("will parse yml...");
            // match serde_yaml::from_str::<HashMap<String, RNT>>(cfg2) {
            //     Err(e) => {
            //         println!("--------- parsed failed e: {:?}", e);
            //     }
            //     Ok(rs) => {
            //         println!("+++++++ parsed success: {:?}", rs);
            //     }
            // }
        });

        println!("succeed!!")
    }
}
