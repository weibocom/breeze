use super::Record;
pub(super) struct Lookup {
    resolver: TokioAsyncResolver,
}

use trust_dns_resolver::{lookup_ip::LookupIp, TokioAsyncResolver};

use std::net::IpAddr;
impl Lookup {
    pub(super) fn new() -> Self {
        Self {
            resolver: TokioAsyncResolver::tokio_from_system_conf()
                .expect("Failed to create the resolver"),
        }
    }
    async fn lookup(&self, host: &str) -> std::io::Result<IpAddrLookup> {
        let ips = self.resolver.lookup_ip(host).await?;
        Ok(IpAddrLookup::LookupIp(ips))
    }
    fn dns_lookup(&self, host: &str) -> std::io::Result<IpAddrLookup> {
        let ips = dns_lookup::lookup_host(host)?;
        Ok(IpAddrLookup::AddrInfoIter(ips))
    }
    pub(super) async fn lookups<'a, I>(&self, iter: I) -> (usize, Option<String>)
    where
        I: Iterator<Item = (&'a str, &'a mut Record)>,
    {
        let mut num = 0;
        let mut cache = None;
        for (host, r) in iter {
            let ret = self.dns_lookup(host);
            if ret.is_err() {
                log::error!("Failed to lookup ip for {} err:{:?}", host, ret.err());
                break;
            }
            let ips = ret.unwrap();
            if r.refresh(host, ips) {
                num += 1;
                if num == 1 {
                    assert!(cache.is_none());
                    cache = Some(host.to_string());
                }
            }
        }
        (num, cache)
    }
}

pub(super) enum IpAddrLookup {
    LookupIp(LookupIp),
    AddrInfoIter(Vec<IpAddr>),
}
impl IpAddrLookup {
    pub(super) fn visit_v4(&self, mut v: impl FnMut(std::net::Ipv4Addr)) {
        match self {
            Self::LookupIp(ips) => ips.iter().for_each(|ip| {
                if let IpAddr::V4(ip) = ip {
                    v(ip);
                }
            }),
            Self::AddrInfoIter(iter) => {
                for ip in iter.iter() {
                    if let std::net::IpAddr::V4(ip) = ip {
                        v(*ip);
                    }
                }
            }
        }
    }
}
