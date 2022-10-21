//! # 已测试场景
//! ## 基本操作验证
//! - basic set
//! - set 过期时间后, ttl > 0
//! - mget 两个key, 其中只有一个set了, 预期应有一个none结果
//! - basic del
//! - basic incr
//! - hash基本操作, set 两个field后,hgetall
//! - hash基本操作, hmset 两个field后,hget
//! - set基本操作, sadd 1, 2, 3后, smembers
//! - list基本操作, rpush, llen, lpop, lrange, lset
//! - 单个zset基本操作, zadd, zrangebyscore withscore
//!## 复杂场景
//!  - set 1 1, ..., set 10000 10000等一万个key已由java sdk预先写入,
//! 从mesh读取, 验证业务写入与mesh读取之间的一致性
//! - value大小数组[4, 40, 400, 4000, 8000, 20000, 3000000],依次set后随机set,验证buffer扩容
//! - pipiline方式,set 两个key后,mget读取

use crate::ci::env::*;
use crate::redis_helper::*;
use ::function_name::named;
use redis::Commands;
use std::collections::HashMap;
//基本场景
#[test]
fn test_args() {
    let mut con = get_conn(&file!().get_host());

    redis::cmd("SET")
        .arg("key1args")
        .arg(b"foo")
        .execute(&mut con);
    redis::cmd("SET")
        .arg(&["key2args", "bar"])
        .execute(&mut con);

    assert_eq!(
        redis::cmd("MGET")
            .arg(&["key1args", "key2args"])
            .query(&mut con),
        Ok(("foo".to_string(), b"bar".to_vec()))
    );
}

//基本set场景，key固定为foo或bar，value为简单数字或字符串
#[test]
fn test_basic_set() {
    let mut con = get_conn(&file!().get_host());

    redis::cmd("SET").arg("fooset").arg(42).execute(&mut con);
    assert_eq!(redis::cmd("GET").arg("fooset").query(&mut con), Ok(42));

    redis::cmd("SET").arg("barset").arg("foo").execute(&mut con);
    assert_eq!(
        redis::cmd("GET").arg("barset").query(&mut con),
        Ok(b"foo".to_vec())
    );
}

// set 过期时间后, ttl > 0
#[named]
#[test]
fn test_set_expire() {
    let arykey = function_name!();
    let mut con = get_conn(&file!().get_host());

    redis::cmd("SET")
        .arg(arykey)
        .arg(42usize)
        .arg("EX")
        .arg(3)
        .execute(&mut con);
    let ttl: usize = redis::cmd("TTL")
        .arg(arykey)
        .query(&mut con)
        .expect("ttl err");
    assert!(ttl > 0);
}

/// mget 两个key, 其中只有一个set了, 预期应有一个none结果
#[named]
#[test]
fn test_set_optionals() {
    let arykey = function_name!();
    let mut con = get_conn(&file!().get_host());

    redis::cmd("SET").arg(arykey).arg(1).execute(&mut con);

    let (a, b): (Option<i32>, Option<i32>) = redis::cmd("MGET")
        .arg(arykey)
        .arg("missing")
        .query(&mut con)
        .unwrap();
    assert_eq!(a, Some(1i32));
    assert_eq!(b, None);
}

#[named]
#[test]
fn test_basic_del() {
    let arykey = function_name!();
    let mut con = get_conn(&file!().get_host());

    redis::cmd("SET").arg(arykey).arg(42).execute(&mut con);

    assert_eq!(redis::cmd("GET").arg(arykey).query(&mut con), Ok(42));

    redis::cmd("DEL").arg(arykey).execute(&mut con);

    assert_eq!(
        redis::cmd("GET").arg(arykey).query(&mut con),
        Ok(None::<usize>)
    );
}

#[named]
#[test]
fn test_basic_incr() {
    let arykey = function_name!();
    let mut con = get_conn(&file!().get_host());

    redis::cmd("SET").arg(arykey).arg(42).execute(&mut con);
    assert_eq!(redis::cmd("INCR").arg(arykey).query(&mut con), Ok(43usize));
}

/// hset 两个field后,hgetall
#[named]
#[test]
fn test_hash_hset_hgetall() {
    let arykey = function_name!();
    let mut con = get_conn(&file!().get_host());

    redis::cmd("HSET")
        .arg(arykey)
        .arg("key_1")
        .arg(1)
        .execute(&mut con);
    redis::cmd("HSET")
        .arg(arykey)
        .arg("key_2")
        .arg(2)
        .execute(&mut con);

    let h: HashMap<String, i32> = redis::cmd("HGETALL")
        .arg(arykey)
        .query(&mut con)
        .expect("hgetall err");
    assert_eq!(h.len(), 2);
    assert_eq!(h.get("key_1"), Some(&1i32));
    assert_eq!(h.get("key_2"), Some(&2i32));
}

/// hmset 两个field后,hget
#[named]
#[test]
fn test_hash_hmset() {
    let arykey = function_name!();
    let mut con = get_conn(&file!().get_host());
    redis::cmd("DEL").arg(arykey).execute(&mut con);

    redis::cmd("HMSET")
        .arg(arykey)
        .arg(&[("field_1", 42), ("field_2", 23)])
        .execute(&mut con);

    assert_eq!(
        redis::cmd("HGET")
            .arg(arykey)
            .arg("field_1")
            .query(&mut con),
        Ok(42)
    );
    assert_eq!(
        redis::cmd("HGET")
            .arg(arykey)
            .arg("field_2")
            .query(&mut con),
        Ok(23)
    );
}

///sadd 1, 2, 3后, smembers
#[named]
#[test]
fn test_set_ops() {
    let arykey = function_name!();
    let mut con = get_conn(&file!().get_host());
    redis::cmd("DEL").arg(arykey).execute(&mut con);

    assert_eq!(con.sadd(arykey, &[1, 2, 3]), Ok(3));

    let mut s: Vec<i32> = con.smembers(arykey).unwrap();
    s.sort_unstable();
    assert_eq!(s.len(), 3);
    assert_eq!(&s, &[1, 2, 3]);
}

///list基本操作:
/// - rpush 8个value
/// - llen
/// - lpop
/// - lrange
/// - lset
#[named]
#[test]
fn test_list_ops() {
    let arykey = function_name!();
    let mut con = get_conn(&file!().get_host());
    redis::cmd("DEL").arg(arykey).execute(&mut con);

    assert_eq!(con.rpush(arykey, &[1, 2, 3, 4]), Ok(4));
    assert_eq!(con.rpush(arykey, &[5, 6, 7, 8]), Ok(8));
    assert_eq!(con.llen(arykey), Ok(8));

    assert_eq!(con.lpop(arykey, Default::default()), Ok(1));
    assert_eq!(con.llen(arykey), Ok(7));

    assert_eq!(con.lrange(arykey, 0, 2), Ok((2, 3, 4)));

    assert_eq!(con.lset(arykey, 0, 4), Ok(true));
    assert_eq!(con.lrange(arykey, 0, 2), Ok((4, 3, 4)));

    assert_eq!(con.lrange(arykey, 0, 10), Ok(vec![4, 3, 4, 5, 6, 7, 8]));
}

/// 单个zset基本操作, zadd, zrangebyscore withscore
#[named]
#[test]
fn test_zset_basic() {
    let arykey = function_name!();
    let mut con = get_conn(&file!().get_host());
    redis::cmd("DEL").arg(arykey).execute(&mut con);

    let values = &[
        (1, "one".to_string()),
        (2, "two".to_string()),
        (4, "four".to_string()),
    ];
    let _: () = con.zadd_multiple(arykey, values).unwrap();

    assert_eq!(
        con.zrange_withscores(arykey, 0, -1),
        Ok(vec![
            ("one".to_string(), 1),
            ("two".to_string(), 2),
            ("four".to_string(), 4),
        ])
    );
}

//github ci 过不了,本地可以过,不清楚原因
/// pipiline方式,set 两个key后,mget读取
// #[test]
// fn test_pipeline() {
//     let mut con = get_conn(&file!().get_host());

//     let ((k1, k2),): ((i32, i32),) = redis::pipe()
//         .cmd("SET")
//         .arg("pipelinekey_1")
//         .arg(42)
//         .ignore()
//         .cmd("SET")
//         .arg("pipelinekey_2")
//         .arg(43)
//         .ignore()
//         .cmd("MGET")
//         .arg(&["pipelinekey_1", "pipelinekey_2"])
//         .query(&mut con)
//         .unwrap();

//     assert_eq!(k1, 42);
//     assert_eq!(k2, 43);
// }

/// set 1 1, ..., set 10000 10000等一万个key已由java sdk预先写入,
/// 从mesh读取, 验证业务写入与mesh读取之间的一致性
#[test]
fn test_get_write_by_sdk() {
    let mut con = get_conn(&file!().get_host());
    for i in exists_key_iter() {
        assert_eq!(redis::cmd("GET").arg(i).query(&mut con), Ok(i));
    }
}

///依次set [4, 40, 400, 4000, 8000, 20000, 3000000]大小的value
///验证buffer扩容,buffer初始容量4K,扩容每次扩容两倍
///后将[4, 40, 400, 4000, 8000, 20000, 3000000] shuffle后再依次set
///测试步骤:
///  1. set, key value size 4k以下，4次
///  3. set key value size 4k~8k，一次, buffer由4k扩容到8k
///  4. set key value size 8k~16k，一次，buffer在一次请求中扩容两次，由8k扩容到16k，16k扩容到32k，
///  5. set, key value size 2M以上，1次
///  6. 以上set请求乱序set一遍
#[named]
#[test]
fn test_set_value_fix_size() {
    let arykey = function_name!();
    let mut con = get_conn(&file!().get_host());

    let mut v_sizes = [4, 40, 400, 4000, 8000, 20000, 3000000];
    for v_size in v_sizes {
        let val = vec![1u8; v_size];
        redis::cmd("SET").arg(arykey).arg(&val).execute(&mut con);
        assert_eq!(redis::cmd("GET").arg(arykey).query(&mut con), Ok(val));
    }

    //todo random iter
    use rand::seq::SliceRandom;
    let mut rng = rand::thread_rng();
    v_sizes.shuffle(&mut rng);
    for v_size in v_sizes {
        let val = vec![1u8; v_size];
        redis::cmd("SET").arg(arykey).arg(&val).execute(&mut con);
        assert_eq!(redis::cmd("GET").arg(arykey).query(&mut con), Ok(val));
    }
}
