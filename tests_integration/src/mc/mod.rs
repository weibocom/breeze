mod basic;
use crate::ci::env::{exists_key_iter, Mesh};
use crate::mc_helper::*;
use memcache::{Client, MemcacheError};
use std::collections::HashMap;

/// 测试场景：buffer扩容验证: 同一个连接，同一个key, set不同大小的value
/// 特征:    key；固定为"fooset"  value: 不同长度的String,内容固定: 每个字符内容为 ‘A’
///
/// 测试步骤：
///     <1> 建立连接
///     <2> 乱序set，先set 1M的value,再乱序set其他大小的value [1048507, 4, 4000, 40, 8000, 20000, 0, 400]
///     <3> 将set进去的value get出来，对比set进去的值与get出来的值；
///     <4> 由小到大分别set不同大小的value，从0开始递增，覆盖从4k->8k, 8k->32k，以及 1M 的场景，
///     <5> 重复步骤 <3>
#[test]
fn buffer_capacity_a() {
    let client = mc_get_conn("mc");
    let key = "fooset";
    let mut v_sizes = [1048507, 4, 4000, 40, 8000, 20000, 0, 400];
    for v_size in v_sizes {
        let val = vec![0x41; v_size];
        assert_eq!(
            client
                .set(key, &String::from_utf8_lossy(&val).to_string(), 2)
                .is_ok(),
            true
        );
        let result: Result<Option<String>, MemcacheError> = client.get(key);
        assert!(result.is_ok());
        assert_eq!(
            result.expect("ok").expect("ok"),
            String::from_utf8_lossy(&val).to_string()
        );
    }
    v_sizes = [0, 4, 40, 400, 4000, 8000, 20000, 1048507];
    for v_size in v_sizes {
        let val = vec![0x41; v_size];
        assert!(client
            .set(key, &String::from_utf8_lossy(&val).to_string(), 2)
            .is_ok());
        let result: Result<Option<String>, MemcacheError> = client.get(key);
        assert!(result.is_ok());
        assert_eq!(
            result.expect("ok").expect("ok"),
            String::from_utf8_lossy(&val).to_string()
        );
    }
}

/// 测试场景：针对目前线上存在的业务方写，mesh 读数据的场景进行模拟，验证数据一致性；
/// 特征:    预先通过java SDK 直接向后端资源写入10000条数据
///          key: “1”.."10000" value: 1..10000
/// 测试步骤：根据已知的 key&value,通过 mesh 获取并对比结果
#[test]
fn only_get_value() {
    let client = mc_get_conn("mc");
    let mut key: String;
    for value in exists_key_iter() {
        key = value.to_string();
        let result: Result<Option<u64>, MemcacheError> = client.get(&key);
        assert!(result.is_ok());
        assert_eq!(result.expect("ok").expect("ok"), value);
    }
}

/// 测试场景: 测试mc 单次最多gets 多少个key
#[test]
fn mc_gets_keys() {
    let client = mc_get_conn("mc");
    let mut key: Vec<String> = Vec::new();
    for keycount in 10001..=11000u64 {
        let k = keycount.to_string();
        let s = client.set(&k, keycount, 1000);
        assert!(s.is_ok(), "panic res:{:?}", s);
        key.push(k);
    }

    let mut mkey: Vec<&str> = Vec::new();
    for (_count, k) in key.iter().enumerate() {
        mkey.push(k.as_str());
    }
    let result: Result<HashMap<String, String>, MemcacheError> = client.gets(mkey.as_slice());
    assert!(result.is_ok());
    assert_eq!(result.expect("ok").len(), 1000);
}

/// 测试场景: 测试key的长度
#[test]
fn mc_key_length() {
    let client = mc_get_conn("mc");
    for k_len in 1..=251usize {
        let key = vec![0x42; k_len];
        let set_res = client.set(
            &String::from_utf8_lossy(&key).to_string(),
            k_len.to_string(),
            2,
        );
        if k_len < 251 {
            assert!(set_res.is_ok());
        } else {
            assert!(set_res.is_err())
        }
        let result: Result<Option<String>, MemcacheError> =
            client.get(&String::from_utf8_lossy(&key).to_string());
        if k_len < 251 {
            assert!(result.is_ok());
            assert_eq!(result.expect("ok").expect("ok"), k_len.to_string());
        } else {
            assert!(result.is_err())
        }
    }
}
