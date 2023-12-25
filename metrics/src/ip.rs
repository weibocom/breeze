use once_cell::sync::OnceCell;

pub(crate) const TARGET_SPLIT: u8 = b'/';
#[inline]
pub(crate) fn encode_addr(addr: &str) -> &str {
    addr
}

// 通过建立一次连接获取本地通讯的IP
static LOCAL_IP_BY_CONNECT: OnceCell<String> = OnceCell::new();
static RAW_LOCAL_IP_BY_CONNECT: OnceCell<String> = OnceCell::new();
lazy_static! {
    static ref LOCAL_IP_STATIC: String =
        local_ip_address::local_ip().expect("local ip").to_string();
}

pub fn local_ip() -> &'static str {
    LOCAL_IP_BY_CONNECT.get_or_init(|| LOCAL_IP_STATIC.to_owned())
}
pub fn raw_local_ip() -> &'static str {
    RAW_LOCAL_IP_BY_CONNECT.get_or_init(|| LOCAL_IP_STATIC.to_owned())
}

use std::io::Result;
use std::net::TcpStream;

fn _init_local_ip(addr: &str) -> Result<()> {
    let local = TcpStream::connect(addr)?.local_addr()?.ip().to_string();

    let raw = local.clone();
    #[cfg(feature = "mock-local-ip")]
    let raw = std::env::var("MOCK_LOCAL_IP").unwrap_or(raw);
    log::info!("local ip inited: {} => {}", raw, local);
    let _ = RAW_LOCAL_IP_BY_CONNECT.set(raw);
    let _ = LOCAL_IP_BY_CONNECT.set(local);
    Ok(())
}
pub fn init_local_ip(addr: &str) {
    if let Err(_e) = _init_local_ip(addr) {
        log::info!(
            "local ip init failed by connecting to {}, use {:?} as local ip. err:{:?}",
            addr,
            LOCAL_IP_STATIC.to_owned(),
            _e
        );
    }
}
