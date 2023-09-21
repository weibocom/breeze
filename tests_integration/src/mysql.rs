use crate::mc_helper::*;
use memcache::MemcacheError;
use std::{thread::sleep, time::Duration};

// Invalid arg异常参数对应的响应
const ERR_INVALID_ARG: &str = "Invalid arguments provided";
// NotStored 异常对应的响应，code 5 对应 RespStatus::NotStored
const ERR_NOT_STORED: &str = "Unknown error occurred with code: 5";

//val中有非assic字符和需要mysql转义的字符
#[test]
#[rustfmt::skip]
fn set() {
    let client = mc_get_conn("mysql");
    let key = "4892225613598465";
    let val =[24, 6, 0, 152, 7, 0, 160, 7, 0, 200, 7, 0, 208, 7, 0, 216, 7, 0, 152, 9, 1, 192, 9, 0, 200, 9, 0, 216, 9, 0, 224, 9, 3, 232, 9, 0, 240, 9, 0, 248, 9, 0, 128, 10, 0, 136, 10, 0, 152, 10, 0, 242, 10, 24, 123, 34, 105, 115, 95, 115, 109, 97, 108, 108, 95, 118, 105, 100, 101, 111, 34, 58, 102, 97, 108, 115, 101, 125, 152, 11, 0, 200, 11,
        '\x1a' as u8,
        '\r' as u8,
        '\n' as u8,
        '\x00' as u8,
        '\'' as u8,
        '\\' as u8,
        '"' as u8,
    ];
    client.add(key, val.as_ref(), 10000).unwrap();
    let result: Result<Option<Vec<u8>>, MemcacheError> = client.get(key);
    assert_eq!(val.as_ref(), result.unwrap().unwrap());
}

#[test]
fn update() {
    let client = mc_get_conn("mysql");
    let key = "4892225613598454";

    client.add(key, "1", 10000).unwrap();
    let result: Result<Option<String>, MemcacheError> = client.get(key);
    assert_eq!("1", result.unwrap().unwrap());

    client.set(key, "2", 10000).unwrap();
    let result: Result<Option<String>, MemcacheError> = client.get(key);
    assert_eq!("2", result.unwrap().unwrap());
}

#[test]
fn delete() {
    let client = mc_get_conn("mysql");
    let key = "4892225613598453";

    client.add(key, "1", 10000).unwrap();
    assert_eq!("1", client.get::<String>(key).unwrap().unwrap());

    client.delete(key).unwrap();
    assert_eq!(None, client.get::<String>(key).unwrap());
}

//ci环境的mysql默认的 max_allowed_packet为4194304，content colume 长度为8KB,但针对2<<16-1的MAX_PAYLOAD_LEN也测试过
const MAX_PAYLOAD_LEN: usize = 8 * 1024;
//构建一个sql长度为MAX_PAYLOAD_LEN的packet
#[test]
fn set_huge_payload() {
    //16777299
    let client = mc_get_conn("mysql");
    let key = "4892225613598444";
    //当val长度为MAX_PAYLOAD_LEN - 1 - 76，构建出来的insert语句长度恰好为MAX_PAYLOAD_LEN
    let val = vec!['a' as u8; MAX_PAYLOAD_LEN];
    client.add(key, val.as_slice(), 10000).unwrap();
    sleep(Duration::from_secs(3));
    let result: Result<Option<Vec<u8>>, MemcacheError> = client.get(key);
    let result = result.unwrap().unwrap();
    assert_eq!(val, result);
}

// 服务端默认的支持的最大sql长度为16MB，基本等同于MAX_PAYLOAD_LEN，所以我们不支持多个packet的sql语句发送
// #[test]
// fn set_over_max_payload() {
//     //16777299
//     let client = mc_get_conn("mysql");
//     let key = "4892225613598445";
//     let val = vec!['a' as u8; MAX_PAYLOAD_LEN * 2];
//     client.add(key, val.as_slice(), 10000).unwrap();
//     sleep(Duration::from_secs(3));
//     let result: Result<Option<Vec<u8>>, MemcacheError> = client.get(key);
//     let result = result.unwrap().unwrap();
//     assert_eq!(val[..8196], result);
// }

#[test]
fn update_not_exsit() {
    let client = mc_get_conn("mysql");
    let key = "4892225613598445";
    client.delete(key).unwrap(); // 确保key不存在
    client.set(key, "2", 10000).unwrap();
    let result: Result<Option<String>, MemcacheError> = client.get(key);
    assert_eq!(None, result.unwrap());
}

#[test]
fn delete_not_exsit() {
    let client = mc_get_conn("mysql");
    let key = "4892225613598446";
    let result = client.delete(key).unwrap();
    assert!(result, "delete not exsit result {:?}", result)
}

#[test]
fn get_invalid_key() {
    let client = mc_get_conn("mysql");
    let key = "9527";
    let result: Result<Option<String>, MemcacheError> = client.get(key);
    assert!(result.is_err_and(|e| e.to_string().contains(ERR_INVALID_ARG)));
}

#[test]
#[should_panic]
#[ignore]
fn insert_invalid_key() {
    let client = mc_get_conn("mysql");
    let key = "9527";
    let val: [u8; 16] = [
        138, 6, 245, 231, 216, 187, 165, 139, 129, 8, 133, 6, 4, 50, 98, 98,
    ];
    client.add(key, val.as_ref(), 10000).unwrap();
}
#[test]
#[should_panic]
#[ignore]
fn insert_null_key() {
    let client = mc_get_conn("mysql");
    let key = "";
    client.add(key, "abcd", 10000).unwrap();
}
#[test]
#[should_panic]
#[ignore]
fn insert_char_key() {
    let client = mc_get_conn("mysql");
    let key = "48922256135984cc";
    client.add(key, "abcd", 10000).unwrap();
}
#[test]
#[should_panic]
#[ignore]
fn insert_long_key() {
    let client = mc_get_conn("mysql");
    let key = std::iter::repeat('5').take(1024).collect::<String>();
    client.add(key.as_str(), "abcd", 10000).unwrap();
}

#[test]
#[should_panic]
#[ignore]
fn update_invalid_key() {
    let client = mc_get_conn("mysql");
    let key = "9527";
    client.set(key, "val", 10000).unwrap();
}
#[test]
#[should_panic]
#[ignore]
fn update_null_key() {
    let client = mc_get_conn("mysql");
    let key = "";
    client.add(key, "abcd", 10000).unwrap();
}
#[test]
#[should_panic]
#[ignore]
fn update_char_key() {
    let client = mc_get_conn("mysql");
    let key = "48922256135984cc";
    client.set(key, "abcd", 10000).unwrap();
}
#[test]
#[should_panic]
#[ignore]
fn update_long_key() {
    let client = mc_get_conn("mysql");
    let key = std::iter::repeat('5').take(1024).collect::<String>();
    client.set(key.as_str(), "abcd", 10000).unwrap();
}

#[test]
fn delete_invalid_key() {
    let client = mc_get_conn("mysql");
    let key = "9527";
    let r = client.delete(key);
    assert!(r.is_err_and(|e| {
        // kv delete 异常，返回 NotStored = 0x0005,
        println!("kv delete err:{}", e.to_string());
        e.to_string().contains(ERR_NOT_STORED)
    }));
    // assert!(!r, "delete invalid key result: {:?}", r);
}
#[test]
#[should_panic]
#[ignore]
fn delete_null_key() {
    let client = mc_get_conn("mysql");
    let key = "";
    client.delete(key).unwrap();
}
#[test]
#[should_panic]
#[ignore]
fn delete_char_key() {
    let client = mc_get_conn("mysql");
    let key = "48922256135984cc";
    client.delete(key).unwrap();
}
#[test]
#[should_panic]
#[ignore]
fn delete_long_key() {
    let client = mc_get_conn("mysql");
    let key = std::iter::repeat('5').take(1024).collect::<String>();
    client.delete(key.as_str()).unwrap();
}
#[test]
#[should_panic]
#[ignore]
fn sql_inject_get() {
    let client = mc_get_conn("mysql");
    let key = "4892225613598465";
    let _ = client.delete(key); // 确保key不存在
    let _ = client.add(key, "abcd", 10000);

    let key_inject = "4892225613598465 or 1=1 --";
    let result: Result<Option<Vec<u8>>, MemcacheError> = client.get(key_inject);
    result.unwrap();
}
