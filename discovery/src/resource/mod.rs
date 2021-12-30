mod redis;
use std::{
    collections::{HashMap, HashSet},
    io::{Error, ErrorKind, Result},
};

use ds::DnsResolver;
use protocol::Resource;

use self::redis::RedisNamespace;

pub(crate) async fn parse_cfg_hosts(
    dns_resolver: &DnsResolver,
    resource: &Resource,
    cfg: &str,
) -> Result<HashMap<String, HashSet<String>>> {
    match resource {
        Resource::Memcache => Ok(HashMap::with_capacity(0)),
        Resource::Redis => RedisNamespace::parse_hosts(dns_resolver, cfg).await,
    }
}

// 根据host查询对应ips
pub(crate) async fn lookup_host(dns_resolver: &DnsResolver, host: &str) -> Result<HashSet<String>> {
    dns_resolver.lookup_ips(host).await
}

// 根据若干hosts查询所有ips
pub(crate) async fn lookup_hosts(
    dns_resolver: &DnsResolver,
    hosts: Vec<&String>,
) -> Result<HashMap<String, HashSet<String>>> {
    let mut all_ips = HashMap::with_capacity(hosts.len());
    for h in hosts {
        match lookup_host(dns_resolver, h).await {
            Ok(ips) => {
                debug_assert!(ips.len() > 0);
                all_ips.insert(h.to_string(), ips);
            }
            Err(e) => {
                log::warn!("parse host for {} failed: {:?}", h, e);
                return Err(Error::new(ErrorKind::AddrNotAvailable, e));
            }
        }
    }
    Ok(all_ips)
}
pub fn name_kind(resource: Resource) -> &'static str {
    match resource {
        Resource::Memcache => "mc",
        Resource::Redis => "redis",
    }
}

// pub(crate) async fn kindof_database(
//     resource: Resource,
//     cfg: &String,
// ) -> HashMap<String, Vec<String>> {
//     match resource {
//         Resource::Memcache => return HashMap::with_capacity(0),
//         Resource::Redis => match RedisNamespace::parse_hosts(cfg).await {
//             Ok(hosts) => return hosts,
//             Err(e) => {
//                 log::warn!("parse redis config failed: {:?}", e);
//                 return HashMap::with_capacity(0);
//             }
//         },
//     }
//}
