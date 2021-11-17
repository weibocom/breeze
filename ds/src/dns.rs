use std::{net::IpAddr, sync::Arc};

use trust_dns_resolver::Resolver;

#[derive(Clone)]
pub struct DnsResolver {
    resolver: Arc<Resolver>,
}

impl DnsResolver {
    pub fn with_sysy_conf() -> Self {
        Self {
            resolver: Arc::new(Resolver::from_system_conf().unwrap()),
        }
    }

    // 解析host得到ip，可能是ipv4 也可能是 ipv6
    pub fn lookup_ips(&self, host: &str) -> Vec<IpAddr> {
        let mut addrs = Vec::with_capacity(10);
        match self.resolver.lookup_ip(host) {
            Ok(lips) => {
                for lip in lips.iter() {
                    addrs.push(lip);
                }
            }
            Err(err) => {
                log::warn!("parse dns/{} failed: {:?}", host, err);
                return addrs;
            }
        }
        addrs
    }
}

impl Default for DnsResolver {
    fn default() -> Self {
        DnsResolver::with_sysy_conf()
    }
}
