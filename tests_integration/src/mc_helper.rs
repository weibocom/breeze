use crate::ci::env::Mesh;
use memcache::Client;

pub fn mc_get_conn(restype: &str) -> Client {
    let host_ip = restype.get_host();
    let host = String::from("memcache://") + &host_ip;
    let client = memcache::connect(host);
    assert_eq!(true, client.is_ok());
    return client.expect("ok");
}

//pub fn mc_get_text_conn(restype: &str) -> Client {
//    let host_ip = restype.get_host();
//    let host = String::from("memcache://") + &host_ip + "?protocol=ascii";
//    let client = memcache::Client::connect(host);
//    assert_eq!(true, client.is_ok());
//    return client.expect("ok");
//}
