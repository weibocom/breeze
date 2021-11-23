use std::{
    collections::HashMap,
    io::{Error, ErrorKind, Result},
    sync::Arc,
};

use trust_dns_resolver::{
    config::{ResolverConfig, ResolverOpts},
    AsyncResolver, Resolver, TokioConnection, TokioConnectionProvider, TokioHandle,
};

#[derive(Clone, Debug)]
pub struct DnsResolver {
    // resolver: Arc<Resolver>,
    resolver: Arc<AsyncResolver<TokioConnection, TokioConnectionProvider>>,
}

impl DnsResolver {
    pub fn with_sysy_conf() -> Self {
        Self {
            // resolver: Arc::new(Resolver::from_system_conf().unwrap()),
            resolver: Arc::new(
                AsyncResolver::new(
                    ResolverConfig::default(),
                    ResolverOpts::default(),
                    TokioHandle,
                )
                .expect("failed to create resolver"),
            ),
        }
    }

    // 解析host得到ip，可能是ipv4 也可能是 ipv6
    pub async fn lookup_ips(&self, host: &str) -> Result<Vec<String>> {
        let mut host_real = host;
        let mut port_real = "";
        let host_port: Vec<&str> = host.split(":").collect();
        if host_port.len() > 1 {
            host_real = host_port[0];
            port_real = host_port[1];
        }

        let mut addrs = Vec::with_capacity(8);
        match self.resolver.lookup_ip(host_real).await {
            Ok(lips) => {
                for lip in lips.into_iter() {
                    if port_real.len() > 0 {
                        addrs.push(lip.to_string() + ":" + port_real);
                    } else {
                        addrs.push(lip.to_string());
                    }
                }
            }
            Err(err) => {
                log::warn!("parse dns/{} failed: {:?}", host, err);
                return Err(Error::new(ErrorKind::InvalidData, err));
            }
        };
        Ok(addrs)
    }
}

impl Default for DnsResolver {
    fn default() -> Self {
        DnsResolver::with_sysy_conf()
    }
}

#[cfg(test)]
mod test {
    use tokio::runtime::Runtime;

    use crate::DnsResolver;

    #[test]
    fn test_lookup_dns() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let dns = DnsResolver::with_sysy_conf();
            let host = "baidu.com";
            let ips = dns.lookup_ips(host).await;
            println!("async parse dns/{} ips:", host);
            for ip in ips {
                println!(" {:?}", ip);
            }
        });

        println!("parsed succeed!");
    }
}
