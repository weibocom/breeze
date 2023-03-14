use crate::ci::env::Mesh;
use memcache::{Client, MemcacheError};

pub fn mc_get_conn(restype: &str) -> Client {
    let host_ip = restype.get_host();
    let host = String::from("memcache://") + &host_ip;
    let client = memcache::connect(host);
    assert_eq!(true, client.is_ok());
    return client.expect("ok");
}
