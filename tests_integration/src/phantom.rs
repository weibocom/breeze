use crate::ci::env::*;
use crate::redis_helper::*;
use redis::cmd;
use std::process::Command;
use std::time::SystemTime;
const SHELL_PATH: &str = "./src/shell/phantom.sh";
/// # Phantom 简单介绍
///
/// Phantom 存在性判断的缓存，采用BloomFilter实现。用于存储用户的访问记录
/// Phantom采用Redis协议通信，实现了bfget、bgset、bfmset、bfmset等命令
///  
/// # 单元测试已覆盖场景：
///  
/// - 基本命令:
///     PING 、bfset 、bfget 、bfmget、bfmset
///     
/// - key 合法性
///     
///
/// -

#[test]
fn test_ping() {
    let mut con = get_conn(&file!().get_host());
    let ping_ret: () = redis::cmd("PING").query(&mut con).unwrap();
    assert_eq!(ping_ret, ());
}

// key只支持64位非负整数!!
//
// 验证非法key
// 最小越界
//
#[test]
fn test_bad_key_bfset() {
    let mut con = get_conn(&file!().get_host());
    let rs = cmd("bfset").arg("bad_key").query::<i64>(&mut con);
    assert_eq!(rs, Ok(-2)); //非法key

    let rs = cmd("bfset").arg("9").query::<i64>(&mut con);
    assert_eq!(rs, Ok(-1)); // 最小越界
}

// 正常key bfset 测试

#[test]
fn test_bfset() {
    let mut con = get_conn(&file!().get_host());

    // 获取初测试key 初始值
    let found = cmd("bfget").arg("1111").query::<i64>(&mut con);

    // bfset 返回前值
    let rs = cmd("bfset").arg("1111").query::<i64>(&mut con);
    assert_eq!(rs, Ok(found.unwrap()));
}

#[test]
fn test_bfget() {
    let mut con = get_conn(&file!().get_host());
    let pref_key = rand::random::<u32>();
    let orgin_rs = cmd("bfget").arg(pref_key).query::<i64>(&mut con);
    let _rs = cmd("bfset").arg(pref_key).query::<i64>(&mut con);

    let rs = cmd("bfget").arg(pref_key).query::<i64>(&mut con);
    assert_eq!(rs, Ok(orgin_rs.unwrap() + 1));
}

#[test]
fn test_bfmget() {
    let pref_key = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let mut con = get_conn(&file!().get_host());
    let key2 = pref_key + 10;
    let key3 = pref_key + 20;
    let found = cmd("bfmget")
        .arg(pref_key)
        .arg(key2)
        .arg(key3)
        .query::<Vec<i64>>(&mut con);
    assert_eq!(found, Ok(vec![0, 0, 0]));
}

#[test]
fn test_bfmset() {
    let pref_key = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let mut con = get_conn(&file!().get_host());
    let key2 = pref_key + 30;
    let key3 = pref_key + 40;
    let found = cmd("bfmset")
        .arg(pref_key)
        .arg(key2)
        .arg(key3)
        .query::<Vec<i64>>(&mut con);
    assert_eq!(found, Ok(vec![0, 0, 0]));

    let rs = cmd("bfmset")
        .arg(pref_key)
        .arg(key2)
        .arg(key3)
        .query::<Vec<i64>>(&mut con);
    assert_eq!(rs, Ok(vec![1, 1, 1]));
}

// 后端的变化及实效性
//
// 保持原有配置不变，set 固定值，key会写入固定分片A
// 更新配置:将 backends 列表进行更新
// 等待15s(配置更新时长)， 配置更新完成后
//
// 配置如果更新成功，GET 固定key，value = 0, 表示后端backends 更新成功

#[test]
fn test_config_change_watched() {
    let fixed_key = "100";
    let mut con = get_conn(&file!().get_host());
    // 初始 key:100 会分片到idx：0 分片
    let _rs_set = cmd("bfset").arg(fixed_key).query::<i64>(&mut con);
    let rs_get = cmd("bfget").arg(fixed_key).query::<i64>(&mut con);
    assert!(rs_get.unwrap() > 0);

    // TODO 临时更新phantom 配置
    let remote_ip = std::env::var("remote_ip").unwrap_or_default();
    assert!(remote_ip.len() > 0);

    let cmd_str = SHELL_PATH.to_string();
    let _exec = Command::new(cmd_str.clone())
        .arg(remote_ip.clone())
        .arg("backend_changed")
        .output()
        .expect("sh exec backend_changed error!");

    println!("after:exec:{:?}", _exec);

    // mesh 启动 updating config tick_sec 默认15s ，sleep 16s ,相关元数据变更watch完毕
    std::thread::sleep(std::time::Duration::from_secs(16));

    // 因后端变更变更 初始key写入的分片已经理论上不存在，
    let rs_get = cmd("bfget").arg(fixed_key).query::<i64>(&mut con);
    //assert_eq!(rs_get.unwrap(), 0);
    let _exec = Command::new(cmd_str.clone())
        .arg(remote_ip.clone())
        .arg("recover")
        .output()
        .expect("sh exec recover error!");
}
