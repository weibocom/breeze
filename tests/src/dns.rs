use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};
lazy_static::lazy_static! {
    static ref CACHE: Mutex<HashMap<String, Vec<String>>> = Mutex::new(Default::default());
}
struct Dns;

impl Dns {
    fn get(&mut self) -> MutexGuard<'_, HashMap<String, Vec<String>>> {
        CACHE.try_lock().expect("run in multithread runtime")
    }
    fn insert(&mut self, host: &str, ip: &str) {
        let mut cache = self.get();
        let ips = cache.entry(host.to_string()).or_insert(Vec::new());
        ips.push(ip.to_string());
    }
}

use endpoint::dns::{DnsLookup, IpAddr, Lookup};
impl Lookup for Dns {
    fn lookup(host: &str, mut f: impl FnMut(&[IpAddr])) {
        if let Some(ips) = Dns.get().get(host) {
            let mut addrs = Vec::with_capacity(ips.len());
            for ip in ips {
                addrs.push(ip.parse().unwrap());
            }
            f(&addrs);
        }
    }
}
#[ignore = "暂时注释掉，#439 有修正"]
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
        vec![
            vec![
                "10.0.0.1:8080".to_string(),
                "10.0.0.10:8080".to_string(),
                "10.0.0.6:8080".to_string(),
            ],
            vec!["10.0.0.2:8080".to_string(), "10.0.0.7:8080".to_string(),]
        ]
    );
    //从需要去重
    query[1].push("slave_1_1:8080".to_string());
    Dns.insert("slave_1_1", "10.0.0.7");
    let ips = query.glookup::<Dns>(true, false).unwrap();
    assert_eq!(ips.len(), 2);
    assert_eq!(
        ips,
        vec![
            vec![
                "10.0.0.1:8080".to_string(),
                "10.0.0.10:8080".to_string(),
                "10.0.0.6:8080".to_string(),
            ],
            vec!["10.0.0.2:8080".to_string(), "10.0.0.7:8080".to_string(),]
        ]
    );
    //从需要去重,但可以包含主
    Dns.insert("slave_1", "10.0.0.2");
    let ips = query.glookup::<Dns>(true, false).unwrap();
    assert_eq!(
        ips,
        vec![
            vec![
                "10.0.0.1:8080".to_string(),
                "10.0.0.10:8080".to_string(),
                "10.0.0.6:8080".to_string(),
            ],
            vec![
                "10.0.0.2:8080".to_string(),
                "10.0.0.7:8080".to_string(),
                "10.0.0.2:8080".to_string(),
            ]
        ]
    );
}

#[test]
fn test_ipv4_vec() {
    use discovery::dns::Ipv4Vec;
    assert_eq!(std::mem::size_of::<Ipv4Vec>(), 32);
    use std::net::Ipv4Addr;
    let mut ips = Ipv4Vec::new();
    let ip0: Ipv4Addr = "10.0.0.1".parse().expect("invalid ip");
    ips.push(ip0);
    let mut iter = ips.iter();
    assert_eq!(iter.next(), Some(ip0));
    assert_eq!(iter.next(), None);

    let vec = vec![
        Ipv4Addr::new(10, 0, 0, 1),
        Ipv4Addr::new(10, 0, 0, 2),
        Ipv4Addr::new(10, 0, 0, 3),
        Ipv4Addr::new(10, 0, 0, 4),
        Ipv4Addr::new(10, 0, 0, 5),
        Ipv4Addr::new(10, 0, 0, 6),
        Ipv4Addr::new(10, 0, 0, 7),
        Ipv4Addr::new(10, 0, 0, 8),
    ];
    let mut ips = discovery::dns::Ipv4Vec::new();
    vec.iter().for_each(|ip| ips.push(*ip));
    assert_eq!(ips.len(), 8);
    let mut iter = ips.iter();
    for i in 0..vec.len() {
        assert_eq!(iter.next(), Some(vec[i]));
    }
    let mut other = Ipv4Vec::new();
    vec.iter().rev().for_each(|ip| other.push(*ip));
    assert_eq!(other.len(), 8);
    assert!(ips == other);
}
