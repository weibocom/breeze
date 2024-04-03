use once_cell::sync::OnceCell;

// 通过建立一次连接获取本地通讯的IP
static LOCAL_IP_BY_CONNECT: OnceCell<String> = OnceCell::new();
static RAW_LOCAL_IP_BY_CONNECT: OnceCell<String> = OnceCell::new();
lazy_static! {
    static ref LOCAL_IP_STATIC: String =
        local_ip_address::local_ip().expect("local ip").to_string();
}

pub fn local_ip() -> &'static str {
    LOCAL_IP_BY_CONNECT.get().expect("uninit")
}
pub fn raw_local_ip() -> &'static str {
    RAW_LOCAL_IP_BY_CONNECT.get().expect("uninit")
}

use std::io::Result;
use std::net::{Ipv4Addr, TcpStream};

fn _init_local_ip_by_conn(addr: &str) -> Result<()> {
    let local = TcpStream::connect(addr)?.local_addr()?.ip().to_string();
    log::info!("local ip inited: {}", local);
    LOCAL_IP_BY_CONNECT.set(local).expect("uninit");
    Ok(())
}

//用于计算可用区
fn init_raw_local_ip(ip: &str) {
    let raw = ip.to_owned();
    #[cfg(feature = "mock-local-ip")]
    let raw = std::env::var("MOCK_LOCAL_IP").unwrap_or(raw);
    log::info!("raw local ip inited: {}", raw);
    RAW_LOCAL_IP_BY_CONNECT.set(raw).expect("uninit");
}

pub fn init_local_ip(addr: &str, host_ip: &str) {
    if !host_ip.is_empty() && host_ip.parse::<Ipv4Addr>().is_ok() {
        LOCAL_IP_BY_CONNECT.set(host_ip.to_owned()).expect("uninit");
    } else if let Err(_e) = _init_local_ip_by_conn(addr) {
        let ip_static = LOCAL_IP_STATIC.to_owned();
        LOCAL_IP_BY_CONNECT.set(ip_static.clone()).expect("uninit");
        log::info!(
            "local ip init failed by connecting to {}, use {:?} as local ip. err:{:?}",
            addr,
            ip_static,
            _e
        );
    }
    init_raw_local_ip(local_ip());
}
