use crate::mc_helper::*;
use memcache::MemcacheError;
/// # 已测试场景：
/// - 验证 mesh buffer 扩容，同一个连接，同一个key，set 8次不同大小的String value
///     key: "fooset"  value: 每个字符内容为 ‘A’
///     乱序set: [1048507, 4, 4000, 40, 8000, 20000, 0, 400]
///     顺序set: [0, 4, 40, 400, 4000, 8000, 20000, 1048507]
///
/// - 模拟mc已有10000条数据，通过 mesh 读取，验证数据一致性
///     数据由 java SDK 预先写入：key: 0...9999  value: 0...9999
///  
/// - 模拟简单mc 基本命令:
///     包括: add、get、replace、delete、append、prepend、gets、cas、incr、decr
///     set命令未单独测试；
///
/// -
use std::collections::HashMap;
/// 测试场景：基本的mc add 命令验证
#[test]
fn mc_max_key() {
    let client = mc_get_conn("mc");
    let key = "simplefooadd";
    let value = "bar";
    let result: Result<Option<String>, MemcacheError> = client.get(key);
    assert_eq!(true, result.expect("ok").is_none());
    assert!(client.add(key, value, 10).is_ok());
    let result: Result<Option<String>, MemcacheError> = client.get(key);
    assert_eq!(true, result.is_ok());
    assert_eq!(result.expect("ok").expect("ok"), value);
}
/// 测试场景：基本的mc add 命令验证
#[test]
fn mc_simple_add() {
    let client = mc_get_conn("mc");
    let key = "fooadd";
    let value = "bar";
    let result: Result<Option<String>, MemcacheError> = client.get(key);
    assert_eq!(true, result.expect("ok").is_none());
    assert!(client.add(key, value, 10).is_ok());
    let result: Result<Option<String>, MemcacheError> = client.get(key);
    assert_eq!(true, result.is_ok());
    assert_eq!(result.expect("ok").expect("ok"), value);
}
/// 测试场景：基本的mc get 命令验证
/// 测试步骤：get(key) key 为预先写入的key
#[test]
fn mc_simple_get() {
    let client = mc_get_conn("mc");
    let result: Result<Option<String>, MemcacheError> = client.get("307");
    assert!(result.is_ok());
    assert_eq!(result.expect("ok").expect("ok"), "307");
}
/// 测试场景：基本的mc replace 命令验证
#[test]
fn mc_simple_replace() {
    let client = mc_get_conn("mc");
    let key = "fooreplace";
    let value = "bar";
    assert!(client.set(key, value, 3).is_ok());
    assert_eq!(true, client.replace(key, "baz", 3).is_ok());
    let result: Result<Option<String>, MemcacheError> = client.get(key);
    assert_eq!(result.is_ok(), true);
    assert_eq!(result.expect("ok").expect("ok"), "baz");
}
///测试场景：基本的mc delete 命令验证
#[test]
fn mc_simple_delete() {
    let client = mc_get_conn("mc");
    let key = "foodel";
    let value = "bar";
    let result: Result<Option<String>, MemcacheError> = client.get(key);
    assert_eq!(true, result.expect("ok").is_none());
    assert!(client.add(key, value, 2).is_ok());
    assert!(client.delete(key).is_ok());
    let result: Result<Option<String>, MemcacheError> = client.get(key);
    assert_eq!(true, result.expect("ok").is_none());
}
/*
#[test]
fn only_set_value() {
    let client = mc_get_conn("mc");
    let mut key: String;
    let mut number: u32 = 0;
    while number < 10000 {
        key = number.to_string();
        let result = client.set(key, number, 500);
        assert_eq!(true,result.is_ok());
        number += 1;
    }
}*/
///测试场景：基本的mc append 命令验证
#[test]
fn mc_simple_append() {
    let client = mc_get_conn("mc");
    let key = "append";
    let value = "bar";
    let append_value = "baz";
    assert!(client.set(key, value, 3).is_ok());
    assert!(client.append(key, append_value).is_ok());
    let result: Result<Option<String>, MemcacheError> = client.get(key);
    assert_eq!(result.is_ok(), true);
    assert_eq!(result.expect("ok").expect("ok"), "barbaz");
}
///测试场景：基本的mc prepend 命令验证
#[test]
fn mc_simple_prepend() {
    let client = mc_get_conn("mc");
    let key = "prepend";
    let value = "bar";
    let prepend_value = "foo";
    assert!(client.set(key, value, 3).is_ok());
    assert!(client.prepend(key, prepend_value).is_ok());
    let result: Result<Option<String>, MemcacheError> = client.get(key);
    assert_eq!(result.is_ok(), true);
    assert_eq!(result.expect("ok").expect("ok"), "foobar");
}
///测试场景：基本的mc cas 命令验证
#[test]
fn mc_simple_cas() {
    let client = mc_get_conn("mc");
    assert!(client.set("foocas", "bar", 10).is_ok());
    let getsres: Result<HashMap<String, (Vec<u8>, u32, Option<u64>)>, MemcacheError> =
        client.gets(&["foocas"]);
    assert!(getsres.is_ok());
    let result: HashMap<String, (Vec<u8>, u32, Option<u64>)> = getsres.expect("ok");
    let (_, _, cas) = result.get("foocas").expect("ok");
    let cas = cas.expect("ok");
    assert_eq!(true, client.cas("foocas", "bar2", 10, cas).expect("ok"));
}
///测试场景：基本的mc gets 命令验证
#[test]
fn mc_simple_gets() {
    //多个key
    let client = mc_get_conn("mc");
    let key = "getsfoo";
    let value = "getsbar";
    assert!(client.set(key, value, 2).is_ok());
    assert!(client.set(value, key, 2).is_ok());
    let result: Result<HashMap<String, String>, MemcacheError> =
        client.gets(&["getsfoo", "getsbar"]);
    assert!(result.is_ok());
    assert_eq!(result.expect("ok").len(), 2);
}
///测试场景：基本的mc incr和decr 命令验证
#[test]
fn mc_simple_incr_decr() {
    let client = mc_get_conn("mc");
    let key = "fooinde";
    assert!(client.set(key, 10, 3).is_ok());
    let res = client.increment(key, 5);
    assert!(res.is_ok());
    assert_eq!(res.expect("ok"), 15);
    let res = client.decrement(key, 5);
    assert!(res.is_ok());
    assert_eq!(res.expect("ok"), 10);
}
