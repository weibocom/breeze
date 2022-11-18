//! 需要先指定分片，多条命令配合的测试

use crate::ci::env::*;
use crate::redis::{RESTYPE, RESTYPEWITHSLAVE};
use crate::redis_helper::*;
use function_name::named;
use redis::Commands;

const SERVERS: [[&str; 2]; 4] = [
    ["127.0.0.1:56378", "127.0.0.1:56378"],
    ["127.0.0.1:56379", "127.0.0.1:56379"],
    ["127.0.0.1:56380", "127.0.0.1:56380"],
    ["127.0.0.1:56381", "127.0.0.1:56381"],
];
const SERVERSWITHSLAVE: [[&str; 2]; 4] = [
    ["127.0.0.1:56378", "127.0.0.1:56381"],
    ["127.0.0.1:56378", "127.0.0.1:56380"],
    ["127.0.0.1:56380", "127.0.0.1:56379"],
    ["127.0.0.1:56381", "127.0.0.1:56378"],
];

// crc32local % 4 的分片
// test_shards_1: shards 2
// test_shards_2: shards 0
// test_shards_3: shards 2
// test_shards_4: shards 3
// test_shards_5: shards 1
// test_shards_6: shards 1
// test_shards_7: shards 3
// test_shards_8: shards 2
// test_shards_9: shards 0
// test_shards_10: shards 1
// test_shards_11: shards 3
// test_shards_12: shards 3
// test_shards_13: shards 1
// test_shards_14: shards 0
// test_shards_15: shards 2
// test_shards_16: shards 0
// test_shards_17: shards 2
// test_shards_18: shards 3
// test_shards_19: shards 1
// test_shards_20: shards 2

/// hashrandomq, master + hashkeyq
/// hashrandomqh后通过mesh set，然后直接连接后端读取，其分片应该是随机的
/// 读取100次，每个分片都有set的key, 则测试通过
#[named]
#[test]
fn test_hashrandomq1() {
    let arykey = function_name!();
    let mut con = get_conn(&RESTYPE.get_host());

    for server in SERVERS {
        let mut con = get_conn(server[0]);
        redis::cmd("DEL").arg(arykey).execute(&mut con);
    }
    for _ in 1..=100 {
        con.send_packed_command(&redis::cmd("hashrandomq").get_packed_command())
            .expect("send err");
        assert_eq!(con.set(arykey, 1), Ok(true));
    }
    for server in SERVERS {
        let mut con = get_conn(server[0]);
        assert_eq!(con.get(arykey), Ok(true));
    }
}

/// hashrandomq 和 master联合测试, 使用后端配置如下
/// - 127.0.0.1:56378,127.0.0.1:56381
/// - 127.0.0.1:56378,127.0.0.1:56380
/// - 127.0.0.1:56380,127.0.0.1:56379
/// - 127.0.0.1:56381,127.0.0.1:56378
/// 56379 set某key,因主没有56379端口,
/// 通过hashrandomq get 100次, 应部分有值
/// 通过master + hashrandomq get 100次, 应全部没值, 因master中没有56379
// #[test]
#[named]
fn test_hashrandomq_with_master() {
    let arykey = function_name!();
    let mut con = get_conn(&RESTYPEWITHSLAVE.get_host());

    for server in SERVERSWITHSLAVE {
        let mut con = get_conn(server[1]);
        if server[1].ends_with("56379") {
            assert_eq!(con.set(arykey, 1), Ok(true));
        } else {
            redis::cmd("DEL").arg(arykey).execute(&mut con);
        }
    }

    // 通过hashrandomq get 100次, 应部分有值
    let mut failed = false;
    let mut successed = false;
    for _ in 1..=100 {
        con.send_packed_command(&redis::cmd("hashrandomq").get_packed_command())
            .expect("send err");
        match con.get(arykey) {
            Ok(true) => successed = true,
            Ok(false) => failed = true,
            Err(err) => panic!("get err {:?}", err),
        }
    }
    assert!(failed);
    assert!(successed);

    failed = false;
    successed = false;
    for _ in 1..=100 {
        con.send_packed_command(&redis::cmd("master").get_packed_command())
            .expect("send err");
        con.send_packed_command(&redis::cmd("hashrandomq").get_packed_command())
            .expect("send err");
        match con.get(arykey) {
            Ok(true) => successed = true,
            Ok(false) => failed = true,
            Err(err) => panic!("get err {:?}", err),
        }
    }
    assert!(failed);
    assert!(!successed);
}
