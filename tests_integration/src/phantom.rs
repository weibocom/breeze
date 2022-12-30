use crate::ci::env::*;
use crate::redis_helper::*;
use redis::cmd;
use std::time::SystemTime;

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
    // 避免与其他test的key重复
    let pref_key = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 10000;

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
