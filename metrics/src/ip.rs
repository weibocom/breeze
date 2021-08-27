use once_cell::sync::OnceCell;

pub fn encode_addr(addr: &str) -> String {
    addr.replace(".", "_").replace(":", "_")
}

// 通过建立一次连接获取本地通讯的IP
pub static LOCAL_IP_BY_CONNECT: OnceCell<String> = OnceCell::new();
lazy_static! {
    static ref LOCAL_IP_STATIC: String =
        encode_addr(&local_ip_address::local_ip().expect("local ip").to_string());
}

pub(crate) fn local_ip() -> &'static String {
    LOCAL_IP_BY_CONNECT.get_or_init(|| LOCAL_IP_STATIC.to_owned())
}

use std::io::Result;
use std::net::TcpStream;

fn _init_local_ip(addr: &str) -> Result<()> {
    let local = encode_addr(&(TcpStream::connect(addr)?.local_addr()?.ip().to_string()));

    log::info!("local ip inited:{}", local);
    let _ = LOCAL_IP_BY_CONNECT.set(local);
    Ok(())
}
pub fn init_local_ip(addr: &str) {
    if let Err(e) = _init_local_ip(addr) {
        log::info!(
            "local ip init failed by connecting to {}, use {:?} as local ip. err:{:?}",
            addr,
            LOCAL_IP_STATIC.to_owned(),
            e
        );
    }
}
