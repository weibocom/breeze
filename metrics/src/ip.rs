pub fn encode_addr(addr: &str) -> String {
    addr.replace(".", "_")
}
lazy_static! {
    pub(crate) static ref LOCAL_IP: String =
        encode_addr(&local_ip_address::local_ip().expect("local ip").to_string());
}
