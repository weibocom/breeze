use std::{
    collections::HashMap,
    io::{Error, ErrorKind, Result},
    net::IpAddr,
};

use ds::DnsResolver;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct RedisNamespace {
    pub(crate) basic: Basic,
    backends: Vec<String>,
    pub(crate) master: Vec<String>,
    pub(crate) slaves: Vec<Vec<String>>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Basic {
    #[serde(default)]
    pub(crate) access_mod: String,
    pub(crate) hash: String,
    pub(crate) distribution: String,
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
        match serde_yaml::from_str::<HashMap<String, RedisNamespace>>(group_cfg) {
            Err(e) => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("parse cfg error: {:?}", e),
                ))
            }
            Ok(rs) => rs
                .into_iter()
                .find(|(k, _v)| k == namespace)
                .map(|(_k, mut v)| {
                    v.parse_backends();
                    v
                })
                .ok_or(Error::new(
                    ErrorKind::NotFound,
                    format!("not found namespace:{}", namespace),
                )),
        }
    }

    // 目前先支持dns模式，待走通后且确认ip格式后，再调整支持ips格式 fishermen
    fn parse_backends(&mut self) {
        // TODO 联调需要，暂时在每次parse时构建一个resolver，后续考虑优化（更好的用法见topology/config） fishermen
        let dns_resolver = DnsResolver::with_sysy_conf();
        let mut master: Vec<IpAddr> = Vec::with_capacity(self.backends.len());
        let mut slave_shards: Vec<Vec<IpAddr>> = Vec::with_capacity(self.backends.len());
        for bk in self.backends.clone() {
            let addrs: Vec<&str> = bk.split(",").collect();

            // 解析master
            let master_ips = dns_resolver.lookup_ips(addrs[0]);
            if master_ips.len() == 0 {
                log::warn!("parse config for redis failed: {}", addrs[0]);
                return;
            }
            debug_assert!(master_ips.len() == 1);
            master.push(master_ips[0]);

            // 解析slave dns
            if addrs.len() == 1 {
                continue;
            }
            debug_assert!(addrs.len() == 2);
            let slave_shard_ips = dns_resolver.lookup_ips(addrs[1]);
            if slave_shard_ips.len() == 0 {
                log::warn!("parse config for redis/slave failed: {}", addrs[1]);
                return;
            }
            slave_shards.push(slave_shard_ips);
        }

        // 设置master的ip pool列表
        self.master.clear();
        for m in master {
            self.master.push(m.to_string());
        }

        // 轮询设置slave的ip pool列表
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
            self.slaves.push(pool);
            pidx += 1;
        }
    }

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
