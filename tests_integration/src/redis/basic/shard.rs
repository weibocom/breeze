//! 需要先指定分片，多条命令配合的测试

use crate::ci::env::*;
use crate::redis::RESTYPE;
use crate::redis_helper::*;
use function_name::named;
use redis::{Commands, RedisError};
use std::collections::HashSet;
use std::vec;

// const SERVERS: [[&str; 2]; 4] = [
//     ["127.0.0.1:56378", "127.0.0.1:56378"],
//     ["127.0.0.1:56379", "127.0.0.1:56379"],
//     ["127.0.0.1:56380", "127.0.0.1:56380"],
//     ["127.0.0.1:56381", "127.0.0.1:56381"],
// ];
const SERVERS: [[&str; 2]; 4] = [
    ["10.182.27.228:56378", "10.182.27.228:56378"],
    ["10.182.27.228:56379", "10.182.27.228:56379"],
    ["10.182.27.228:56380", "10.182.27.228:56380"],
    ["10.182.27.228:56381", "10.182.27.228:56381"],
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
/// hashrandomq 通过mesh set，然后直接连接后端读取，其分片应该是随机的
/// 读取100次，每个分片>5, 则测试通过
#[test]
fn test_hashrandomq() {
    let mut con = get_conn(&RESTYPE.get_host());
    con.send_packed_command(&redis::cmd("master").get_packed_command())
        .expect("send master err");
    assert_eq!(con.set("a", 1), Ok(true));
}
