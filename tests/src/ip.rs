use std::net::TcpStream;

use metrics::{init_local_ip, local_ip, raw_local_ip};

//以下三个用例只能一个个运行，因为共享了全局变量
#[cfg_attr(not(feature = "github_workflow"), test)]
fn test_init_local_ip_host_ip_empty() {
    // 场景1：host_ip为空
    std::env::remove_var("MOCK_LOCAL_IP"); // 确保没有设置MOCK_LOCAL_IP环境变量
    init_local_ip("10.10.10.10:53", "");

    let actual_local_ip = TcpStream::connect("10.10.10.10:53")
        .unwrap()
        .local_addr()
        .unwrap()
        .ip()
        .to_string();

    // 验证本地IP已正确初始化且与实际本机IP一致
    assert_eq!(local_ip(), actual_local_ip);

    // 验证原始本地IP已正确初始化且与实际本机IP一致
    assert_eq!(raw_local_ip(), actual_local_ip);
}

#[cfg_attr(not(feature = "github_workflow"), test)]
fn test_init_local_ip_host_ip_not_empty() {
    // 场景2：host_ip不为空

    let test_host_ip = "192.168.1.100";
    init_local_ip("10.10.10.10:53", test_host_ip);

    // 验证本地IP已设置为传入的host_ip
    assert_eq!(local_ip(), test_host_ip);
    assert_eq!(raw_local_ip(), test_host_ip);
}

#[cfg_attr(not(feature = "github_workflow"), test)]
fn test_init_local_ip_host_ip_empty_with_mock_local_ip() {
    // 场景3：host_ip为空，但设置了MOCK_LOCAL_IP环境变量
    std::env::set_var("MOCK_LOCAL_IP", "172.16.0.50");
    init_local_ip("10.10.10.10:53", "");

    let actual_local_ip = TcpStream::connect("10.10.10.10:53")
        .unwrap()
        .local_addr()
        .unwrap()
        .ip()
        .to_string();

    // 验证原始本地IP已被MOCK_LOCAL_IP环境变量覆盖
    assert_eq!(raw_local_ip(), "172.16.0.50");
    assert_eq!(local_ip(), actual_local_ip);
}
