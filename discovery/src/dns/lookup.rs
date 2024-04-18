use super::{Ipv4Vec, Record};
pub(super) struct Lookup {}

use dns_lookup::{getaddrinfo, AddrInfoHints};
use libc::SOCK_STREAM;

use std::net::IpAddr;
impl Lookup {
    pub(super) fn new() -> Self {
        Self {}
    }
    fn dns_lookup(&self, host: &str) -> std::io::Result<Ipv4Vec> {
        let hints = AddrInfoHints {
            socktype: SOCK_STREAM as i32,
            ..AddrInfoHints::default()
        };

        let mut ipv4vec = super::Ipv4Vec::new();

        let addrs = getaddrinfo(Some(host), None, Some(hints))?;
        for ip in addrs {
            let ip = ip?;
            match ip.sockaddr.ip() {
                IpAddr::V4(ip) => ipv4vec.push(ip),
                IpAddr::V6(_) => {}
            }
        }
        Ok(ipv4vec)
    }
    pub(super) fn lookups<'a, I>(&self, iter: I) -> (usize, Option<String>)
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
                (num == 1).then(|| cache = Some(host.to_string()));
            }
        }
        (num, cache)
    }
}

pub type IpAddrLookup = Ipv4Vec;
