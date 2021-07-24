lazy_static! {
    pub(crate) static ref LOCAL_IP: String = local_ip_address::local_ip()
        .expect("local ip")
        .to_string()
        .replace(".", "_");
}
