//! # 已测试场景
//! ## 基本操作验证
//! - basic set
//! - set 过期时间后, ttl > 0
//! - mget 两个key
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
//! - key大小数组[4, 40, 400, 4000], 依次set后get
//! - pipiline方式,set 两个key后,mget读取(注释了,暂未验证)

use crate::ci::env::*;
use crate::redis_helper::*;
use ::function_name::named;
use redis::Commands;
use std::collections::HashMap;
use std::vec;

//获i64
fn find_primes(n: usize) -> Vec<i64> {
    let mut result = Vec::new();
    let mut is_prime = vec![true; n + 1];

    for i in 2..=n {
        if is_prime[i] {
            result.push(i as i64);
        }

        ((i * 2)..=n).into_iter().step_by(i).for_each(|x| {
            is_prime[x] = false;
        });
    }
    //println!("{:?}", is_prime);
    result
}

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
/// - lpush 插入四个 rpush插入4个
/// - lrange 0 -1 获取所有值
/// - lpop 弹出第一个
/// - rpop 弹出最后一个
/// - llen 长度
/// - lrange
/// - lset 将指定位置替换
/// - lindex 获取指定位置值
/// - linsert_before 在指定值之前插入
/// - linsert-after 之后插入
/// - lrange
/// - lrem 移除>0 从head起一个4
/// - lrem 移除<0 从tail 两个7
/// - lrem 移除=0  删除所有2
/// - lrange
/// - ltrim 保留指定区间
/// - lpushx 头插入一个
/// - rpushx 尾插入一个
/// - lrange
#[named]
#[test]
fn test_list_ops() {
    let arykey = function_name!();
    let mut con = get_conn(&file!().get_host());
    redis::cmd("DEL").arg(arykey).execute(&mut con);

    assert_eq!(con.lpush(arykey, &[1, 2]), Ok(2));
    assert_eq!(con.lpush(arykey, &[3, 4]), Ok(4));
    assert_eq!(con.rpush(arykey, &[5, 6]), Ok(6));
    assert_eq!(con.rpush(arykey, &[7, 8]), Ok(8));

    assert_eq!(con.lrange(arykey, 0, -1), Ok((4, 3, 2, 1, 5, 6, 7, 8)));

    assert_eq!(con.lpop(arykey, Default::default()), Ok(4));
    assert_eq!(con.rpop(arykey, Default::default()), Ok(8));
    assert_eq!(con.llen(arykey), Ok(6));
    assert_eq!(con.lrange(arykey, 0, -1), Ok((3, 2, 1, 5, 6, 7)));

    assert_eq!(con.lset(arykey, 0, 4), Ok(true));
    assert_eq!(con.lindex(arykey, 0), Ok(4));

    assert_eq!(con.linsert_before(arykey, 4, 4), Ok(7));
    assert_eq!(con.linsert_after(arykey, 7, 7), Ok(8));
    assert_eq!(con.lrange(arykey, 0, -1), Ok((4, 4, 2, 1, 5, 6, 7, 7)));

    assert_eq!(con.lrem(arykey, 1, 4), Ok(1));
    assert_eq!(con.lrem(arykey, -2, 7), Ok(2));
    assert_eq!(con.lrem(arykey, 0, 2), Ok(1));
    assert_eq!(con.lrange(arykey, 0, -1), Ok((4, 1, 5, 6)));

    assert_eq!(con.ltrim(arykey, 1, 2), Ok(true));
    assert_eq!(con.lpush_exists(arykey, 1), Ok(3));
    assert_eq!(con.rpush_exists(arykey, 5), Ok(4));
    assert_eq!(con.lrange(arykey, 0, -1), Ok((1, 1, 5, 5)));
}

#[named]
#[test]
fn test_list_longset_ops() {
    // let arykey = function_name!();
    let arykey = "xinxin";
    let mut con = get_conn(&file!().get_host());
    // let mut con_ori = get_conn("10.182.27.228:8080");
    // redis::cmd("LSMALLOC").arg(arykey).execute(&mut con_ori);
    // redis::cmd("DEL").arg(arykey).execute(&mut con);
    // println!("{:?}", find_primes(10).capacity());
    // redis::cmd("LSDSET")
    //     .arg(arykey)
    //     .arg(2)
    //     .arg(find_primes(10))
    //     .execute(&mut con);
    // redis::cmd("LSPUT").arg(arykey).arg(2).execute(&mut con);
    // redis::cmd("LSGETALL").arg(arykey).execute(&mut con);
    // redis::cmd("LSDUMP").arg(arykey).arg(2).execute(&mut con);
    // redis::cmd("LSLEN").arg(arykey).execute(&mut con);

    //assert_eq!(con.lsset(arykey, 5), Ok(4));
}

/// - geoadd 添加地理位置经纬度
/// - geodist 获取km级别两个位置距离
/// - geopos 获取地理位置的坐标
/// - geohash：返回一个或多个位置对象的 geohash 值
/// - georadius 根据用户给定的经纬度坐标来获取100km内的地理位置集合,
///   WITHDIST: 在返回位置元素的同时， 将位置元素与中心之间的距离也一并返回,
//    WITHCOORD: 将位置元素的经度和纬度也一并返回。
/// - georadiusbymember 根据储存在位置集合里面的地点获取范围100km的地理位置集合
#[named]
#[test]
fn test_geo_ops() {
    let arykey = function_name!();
    let mut con = get_conn(&file!().get_host());
    redis::cmd("DEL").arg(arykey).execute(&mut con);

    assert_eq!(
        redis::cmd("GEOADD")
            .arg(arykey)
            .arg(30)
            .arg(50)
            .arg("Beijing")
            .arg(27)
            .arg(54)
            .arg("Tianjin")
            .arg(12)
            .arg(15)
            .arg("Hebei")
            .query(&mut con),
        Ok(3)
    );

    assert_eq!(
        redis::cmd("GEODIST")
            .arg(arykey)
            .arg("Beijing")
            .arg("Tianjin")
            .arg("km")
            .query(&mut con),
        Ok(489.9349)
    );

    assert_eq!(
        redis::cmd("GEOPOS")
            .arg(arykey)
            .arg("Beijing")
            .arg("Tianjin")
            .query(&mut con),
        Ok((
            (
                "30.00000089406967163".to_string(),
                "49.99999957172130394".to_string()
            ),
            (
                "26.99999839067459106".to_string(),
                "53.99999994301438733".to_string()
            ),
        ))
    );

    assert_eq!(
        redis::cmd("GEOHASH")
            .arg(arykey)
            .arg("Beijing")
            .arg("Tianjin")
            .query(&mut con),
        Ok(("u8vk6wjr4e0".to_string(), "u9e5nqkuc90".to_string()))
    );
    // operation not permitted on a read only server
    // assert_eq!(
    //     redis::cmd("GEORADIUS")
    //         .arg(arykey)
    //         .arg(28)
    //         .arg(30)
    //         .arg(100)
    //         .arg("km")
    //         .arg("WITHDIST")
    //         .arg("WITHCOORD")
    //         .query(&mut con),
    //     Ok(0)
    // );
    // assert_eq!(
    //     redis::cmd("GEORADIUSBYMEMBER")
    //         .arg(arykey)
    //         .arg("Tianjin")
    //         .arg(100)
    //         .arg("km")
    //         .query(&mut con),
    //     Ok(0)
    // );
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

///依次set key长度为[4, 40, 400, 4000]
#[test]
fn test_set_key_fix_size() {
    let mut con = get_conn(&file!().get_host());

    let key_sizes = [4, 40, 400, 4000];
    for key_size in key_sizes {
        let key = vec![1u8; key_size];
        redis::cmd("SET").arg(&key).arg("foo").execute(&mut con);
        assert_eq!(
            redis::cmd("GET").arg(&key).query(&mut con),
            Ok("foo".to_string())
        );
    }
}

//mget 获取10000个key
#[test]
fn test_mget_10000() {
    let mut con = get_conn(&file!().get_host());

    let maxkey = 10000;
    let mut keys = Vec::with_capacity(maxkey);
    for i in 1..=maxkey {
        keys.push(i);
    }
    assert_eq!(redis::cmd("MGET").arg(&keys).query(&mut con), Ok(keys));
}
