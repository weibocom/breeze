//! 需要先指定分片，多条命令配合的测试
#![allow(unused)]

use std::time::Duration;

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
// test_shards_3: shards 2 已用
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
#[cfg(feature = "github_workflow")]
fn test_hashrandomq1() {
    let argkey = function_name!();
    let mut con = get_conn(&RESTYPE.get_host());

    for server in SERVERS {
        let mut con = get_conn(server[0]);
        redis::cmd("DEL").arg(argkey).execute(&mut con);
    }
    for _ in 1..=100 {
        con.send_packed_command(&redis::cmd("hashrandomq").get_packed_command())
            .expect("send err");
        assert_eq!(con.set(argkey, 1), Ok(true));
    }
    for server in SERVERS {
        let mut con = get_conn(server[0]);
        assert_eq!(con.get(argkey), Ok(true));
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
#[test]
#[named]
#[cfg(feature = "github_workflow")]
fn test_hashrandomq_with_master() {
    let argkey = function_name!();
    let mut con = get_conn(&RESTYPEWITHSLAVE.get_host());

    for server in SERVERSWITHSLAVE {
        let mut con = get_conn(server[1]);
        if server[1].ends_with("56379") {
            assert_eq!(con.set(argkey, 1), Ok(true));
        } else {
            redis::cmd("DEL").arg(argkey).execute(&mut con);
        }
    }

    // 通过hashrandomq get 100次, 应部分有值
    let mut failed = false;
    let mut successed = false;
    for _ in 1..=100 {
        con.send_packed_command(&redis::cmd("hashrandomq").get_packed_command())
            .expect("send err");
        match con.get(argkey) {
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
        match con.get(argkey) {
            Ok(true) => successed = true,
            Ok(false) => failed = true,
            Err(err) => panic!("get err {:?}", err),
        }
    }
    assert!(failed);
    assert!(!successed);
}

/// 先set argkey 1到指定56378端口 set argkey 2 到56381
/// master后从库56381是2 56378是1
/// 56378端口从库是56381正常get是1 但是加完master后是从主库读所以是2
#[named]
#[test]
#[cfg(feature = "github_workflow")]
fn test_master() {
    let argkey = function_name!();
    let mut con = get_conn(&RESTYPEWITHSLAVE.get_host());
    for server in SERVERSWITHSLAVE {
        let mut con = get_conn(server[0]);
        if server[0].ends_with("56378") {
            assert_eq!(con.set(argkey, 1), Ok(true));
        } else if server[0].ends_with("56381") {
            assert_eq!(con.set(argkey, 2), Ok(true));
        } else {
            redis::cmd("DEL").arg(argkey).execute(&mut con);
        }
    }
    con.send_packed_command(&redis::cmd("master").get_packed_command())
        .expect("send err");
    for server in SERVERSWITHSLAVE {
        let mut con1 = get_conn(server[1]);
        if server[1].ends_with("56378") {
            assert_eq!(con1.get(argkey), Ok(1));
        } else if server[1].ends_with("56381") {
            assert_eq!(con1.get(argkey), Ok(2));
        } else {
            assert_eq!(con1.get(argkey), Ok(false));
        }
    }
}

fn hashkey(swallow: bool) {
    let argkey = "test_shards_3";
    let mut con = get_conn(&RESTYPE.get_host());
    for server in SERVERS {
        let mut con = get_conn(server[0]);
        redis::cmd("DEL").arg(argkey).execute(&mut con);
    }

    if swallow {
        con.send_packed_command(
            &redis::cmd("hashkeyq")
                .arg("test_shards_4")
                .get_packed_command(),
        )
        .expect("send err");
    } else {
        assert_eq!(
            redis::cmd("hashkey").arg("test_shards_4").query(&mut con),
            Ok(3)
        );
    }
    //set 到了分片3
    assert_eq!(con.set(argkey, 1), Ok(true));
    //从分片2获取
    assert_eq!(con.get(argkey), Ok(false));

    if swallow {
        con.send_packed_command(
            &redis::cmd("hashkeyq")
                .arg("test_shards_4")
                .get_packed_command(),
        )
        .expect("send err");
    } else {
        assert_eq!(
            redis::cmd("hashkey").arg("test_shards_4").query(&mut con),
            Ok(3)
        )
    }
    assert_eq!(con.get(argkey), Ok(true));
}

/// 利用hashkeyq set落到第三片 hashkeyq + test_sharsd_4+ set argkey 1在第三片"127.0.0.1:56381,127.0.0.1:56381上
/// 然后直接get会从第一片上get get不到
///  再利用hashkeyq + test_sharsd_4+ get argkey去get 成功获取到
#[test]
#[cfg(feature = "github_workflow")]
fn test_hashkeyq() {
    hashkey(true);
}

#[test]
#[cfg(feature = "github_workflow")]
fn test_hashkey() {
    hashkey(false);
}

/// 测试master+hashkeyq
///  set argkey 1
/// master
/// master_hashkeyq_1在第2片  key:test_shards_5在第1片
///
///
///
/// 向56379 set argkey 1
/// get argkey 1 在第二片可以get到
/// 加master get失败 没有56379d的主库 所以获取失败
/// 利用hashkeyq set
/// master hashkeyq get 成功获取到值
#[named]
#[test]
#[cfg(feature = "github_workflow")]
fn master_hashkeyq_1() {
    let argkey = function_name!();
    let mut con = get_conn(&RESTYPEWITHSLAVE.get_host());
    for server in SERVERSWITHSLAVE {
        let mut con = get_conn(server[1]);
        if server[1].ends_with("56379") {
            assert_eq!(con.set(argkey, 1), Ok(true));
        } else {
            redis::cmd("DEL").arg(argkey).execute(&mut con);
        }
    }
    assert_eq!(con.get(argkey), Ok(1));
    con.send_packed_command(&redis::cmd("master").get_packed_command())
        .expect("send err");
    assert_eq!(con.get(argkey), Ok(false));

    con.send_packed_command(
        &redis::cmd("hashkeyq")
            .arg("test_shards_5")
            .get_packed_command(),
    )
    .expect("send err");
    assert_eq!(con.set(argkey, 2), Ok(true));
    // for server in SERVERSWITHSLAVE {
    //     let mut con1 = get_conn(server[1]);
    //     let mut con0 = get_conn(server[0]);
    //     let a: Option<i32> = con0
    //         .get(argkey)
    //         .map_err(|e| panic!("set error:{:?}", e))
    //         .expect("set err");
    //     let a1: Option<i32> = con1
    //         .get(argkey)
    //         .map_err(|e| panic!("set error:{:?}", e))
    //         .expect("set err");
    //     println!("{:?},{:?}", a, a1);
    // }
    con.send_packed_command(&redis::cmd("master").get_packed_command())
        .expect("send err");
    con.send_packed_command(
        &redis::cmd("hashkeyq")
            .arg("test_shards_5")
            .get_packed_command(),
    )
    .expect("send err");
    assert_eq!(con.get(argkey), Ok(2));
}

#[test]
#[cfg(feature = "github_workflow")]
fn test_keyshard() {
    let mut con = get_conn(&RESTYPE.get_host());

    assert_eq!(
        redis::cmd("keyshard")
            .arg(&["test_shards_19", "test_shards_20"])
            .query(&mut con),
        Ok((1, 2))
    );
}

//sendtoall  sendtoallq 命令
#[test]
#[named]
#[cfg(feature = "github_workflow")]
fn test_sendto_all() {
    let argkey = function_name!();
    let mut con = get_conn(&RESTYPE.get_host());

    for server in SERVERS {
        let mut con = get_conn(server[0]);
        redis::cmd("DEL").arg(argkey).execute(&mut con);
    }
    redis::cmd("SENDTOALL").execute(&mut con);
    redis::cmd("SET").arg(argkey).arg(1).execute(&mut con);
    std::thread::sleep(Duration::from_secs(1));
    for server in SERVERS {
        let mut con = get_conn(server[0]);
        assert_eq!(con.get(argkey), Ok(1));
    }

    con.send_packed_command(&redis::cmd("SENDTOALLQ").get_packed_command())
        .expect("send err");
    redis::cmd("DEL").arg(argkey).execute(&mut con);
    std::thread::sleep(Duration::from_secs(1));
    for server in SERVERS {
        let mut con = get_conn(server[0]);
        assert_eq!(con.get(argkey), Ok(false));
    }
}
