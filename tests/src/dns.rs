use std::collections::HashMap;
use std::net::IpAddr;
static mut CACHE: Option<HashMap<String, Vec<String>>> = None;
struct Dns;

impl Dns {
    fn insert(&mut self, host: &str, ip: &str) {
        unsafe {
            let ips = CACHE
                .get_or_insert(Default::default())
                .entry(host.to_string())
                .or_insert(Vec::new());
            ips.push(ip.to_string());
        }
    }
}

use endpoint::dns::{DnsLookup, Lookup};
impl Lookup for Dns {
    fn lookup(host: &str, mut f: impl FnMut(&[IpAddr])) {
        unsafe {
            if let Some(cache) = &CACHE {
                if let Some(ips) = cache.get(host) {
                    let mut addrs = Vec::with_capacity(ips.len());
                    for ip in ips {
                        addrs.push(ip.parse().unwrap());
                    }
                    f(&addrs);
                }
            }
        }
    }
}
#[test]
fn dns_lookup() {
    let mut query: Vec<Vec<String>> = Vec::new();
    assert_eq!(query.glookup::<Dns>(false, true), None);
    // 1. 检查允许为空的情况
    Dns.insert("master_0", "10.0.0.1");
    query.push(vec!["master_0:8080".to_string()]);
    let ips = query.glookup::<Dns>(false, true).unwrap();
    assert_eq!(ips.len(), 1);
    assert_eq!(ips[0], vec!["10.0.0.1:8080".to_string()]);
    // 2. 有空值，检查允许为空的情况
    query.push(vec!["master_1:8080".to_string()]);
    let ips = query.glookup::<Dns>(false, true).unwrap();
    assert_eq!(ips.len(), 2);
    assert_eq!(ips[0], vec!["10.0.0.1:8080".to_string()]);
    assert_eq!(ips[1].len(), 0);
    // 3. 有空值，检查不允许为空的情况
    assert_eq!(query.glookup::<Dns>(false, false), None);
    // 插入master_1的ip
    Dns.insert("master_1", "10.0.0.2");
    let ips = query.glookup::<Dns>(false, false).unwrap();
    assert_eq!(ips.len(), 2);
    assert_eq!(ips[0][0], "10.0.0.1:8080");
    assert_eq!(ips[1][0], "10.0.0.2:8080");
    // 4. 检查master_slave_mode
    // 只有master的ip
    assert_eq!(query.glookup::<Dns>(true, false), None);
    // 有一个有slave
    Dns.insert("slave_0", "10.0.0.10");
    query[0].push("slave_0:8080".to_string());
    assert_eq!(query.glookup::<Dns>(true, false), None);
    // master有2个ip
    Dns.insert("master_0", "10.0.0.3");
    Dns.insert("master_1", "10.0.0.4");
    assert_eq!(query.glookup::<Dns>(true, false), None);
    // 每个都有从
    query[1].push("slave_1:8080".to_string());
    Dns.insert("slave_0", "10.0.0.6");
    Dns.insert("slave_1", "10.0.0.7");
    let ips = query.glookup::<Dns>(true, false).unwrap();
    assert_eq!(ips.len(), 2);
    // master有2个ip，只取最后一个
    assert_eq!(
        ips,
        vec![vec![
            "10.0.0.3:8080".to_string(),
            "10.0.0.10:8080".to_string(),
            "10.0.0.6:8080".to_string(),
        ],
        vec![
        "10.0.0.4:8080".to_string(),
        "10.0.0.7:8080".to_string(),
        ]]
    );
}
