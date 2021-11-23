use ds::DnsResolver;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    io::{Error, ErrorKind, Result},
};

// TODO 用于预处理配置，解析出dns，后续需要放置到更合适的位置，并增加探测机制 fishermen
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct RedisNamespace {
    #[serde(default)]
    pub(crate) basic: Basic,
    #[serde(default)]
    pub(crate) backends: Vec<String>,
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

impl RedisNamespace {
    // 此处只处理域名，不解析
    pub(crate) async fn parse_hosts(cfg: &str) -> Result<HashMap<String, Vec<String>>> {
        log::debug!("redis config: {}", cfg);
        match serde_yaml::from_str::<RedisNamespace>(cfg) {
            Err(e) => {
                log::warn!("parse hosts for redis cfg failed:{:?}", e);
                return Err(Error::new(ErrorKind::AddrNotAvailable, e));
            }
            Ok(rs) => {
                let dns_resolver = DnsResolver::with_sysy_conf();
                let hosts = rs.all_hosts(&dns_resolver).await?;
                Ok(hosts)
            }
        }
    }

    async fn all_hosts(&self, resolver: &DnsResolver) -> Result<HashMap<String, Vec<String>>> {
        let mut host_addrs = HashMap::with_capacity(self.backends.len());
        for b in self.backends.clone() {
            let hosts = b.split(",");
            for h in hosts {
                let ips = resolver.lookup_ips(h).await?;
                host_addrs.insert(h.to_string(), ips);
            }
        }
        Ok(host_addrs)
    }
}
