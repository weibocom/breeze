use super::{Ipv4Vec, Record, Resolver};
pub(super) struct Lookup {
    resolver: Resolver,
}

use std::net::IpAddr;
impl Lookup {
    pub(super) fn new() -> Self {
        Self {
            resolver: Resolver::tokio_from_system_conf().expect("Failed to create the resolver"),
        }
    }
    async fn dns_lookup(&self, host: &str) -> std::io::Result<Ipv4Vec> {
        let mut ipv4vec = super::Ipv4Vec::new();
        match self.resolver.lookup_ip(host.to_string()).await {
            Ok(ips) => {
                for ip in ips.iter() {
                    match ip {
                        IpAddr::V4(ip) => ipv4vec.push(ip),
                        IpAddr::V6(_) => {}
                    }
                }
            }
            Err(e) => log::info!("refresh host failed:{}, {:?}", host, e),
        }
        Ok(ipv4vec)
    }
    pub(super) async fn lookups<'a, I>(&self, iter: I) -> std::io::Result<(usize, Option<String>)>
    where
        I: Iterator<Item = (&'a str, &'a mut Record)>,
    {
        let mut num = 0;
        let mut cache = None;
        for (host, r) in iter {
            let ret = self.dns_lookup(host).await;
            if ret.is_err() {
                log::error!("Failed to lookup ip for {} err:{:?}", host, ret.err());
                break;
            }
            let ips = ret.unwrap();
            if r.refresh(ips) {
                num += 1;
                (num == 1).then(|| cache = Some(host.to_string()));
            }
        }
        Ok((num, cache))
    }
}

pub type IpAddrLookup = Ipv4Vec;
