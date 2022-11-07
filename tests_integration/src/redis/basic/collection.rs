//! 各种数据结构的测试
use crate::ci::env::*;
use crate::redis::RESTYPE;
use crate::redis_helper::*;
use function_name::named;
use redis::{Commands, RedisError};
use std::collections::HashSet;
use std::vec;

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
    let mut con = get_conn(&RESTYPE.get_host());
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
    let mut con = get_conn(&RESTYPE.get_host());
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

/// - hash基本操作
/// - hmset test_hash_ops ("filed1", 1),("filed2", 2),("filed3", 3),("filed4", 4),("filed6", 6),
/// - hgetall test_hash_ops 获取该key下所有字段-值映射表
/// - hdel 删除字段filed1
/// - hlen 获取test_hash_ops表中字段数量 5
/// - hkeys 获取test_hash_ops表中所有字段
/// - hset 设置一个新字段 "filed5", 5 =》1
/// - hset 设置一个旧字段 "filed2", 22 =》0
/// - hmget获取字段filed2 filed5的值
/// - hincrby filed2 4 =>26
/// - hincrbyfloat filed5 4.4 =>8.4
/// - hexists 不存在的filed6=>0
/// - hsetnx不存在的 filed6 =》1
/// - hsetnx已经存在的 filed6 =》0
/// - hvals 获取所有test_hash_ops表filed字段的vals
///   当前filed2 22 filed3 3 filed4 4 filed 5 8.4 filede 6 6
///
/// - hsetnx不存在的 hash表hashkey =》1
/// - hget hash表hashkey hashfiled6=》6  
/// - hscan 获取hash表hashkey 所有的字段和value 的元组
/// - hscan_match 获取hash表test_hash_ops 和filed6相关的元组
#[named]
#[test]
fn test_hash_ops() {
    let arykey = function_name!();
    let mut con = get_conn(&RESTYPE.get_host());
    redis::cmd("DEL").arg(arykey).execute(&mut con);
    redis::cmd("DEL").arg("hashkey").execute(&mut con);

    assert_eq!(
        con.hset_multiple(
            arykey,
            &[("filed1", 1), ("filed2", 2), ("filed3", 3), ("filed4", 4),]
        ),
        Ok(true)
    );
    assert_eq!(
        con.hgetall(arykey),
        Ok((
            "filed1".to_string(),
            1.to_string(),
            "filed2".to_string(),
            2.to_string(),
            "filed3".to_string(),
            3.to_string(),
            "filed4".to_string(),
            4.to_string(),
        ))
    );

    assert_eq!(con.hdel(arykey, "filed1"), Ok(1));
    assert_eq!(con.hlen(arykey), Ok(3));
    assert_eq!(
        con.hkeys(arykey),
        Ok((
            "filed2".to_string(),
            "filed3".to_string(),
            "filed4".to_string(),
        ))
    );

    assert_eq!(con.hset(arykey, "filed5", 5), Ok(1));
    assert_eq!(con.hset(arykey, "filed2", 22), Ok(0));
    assert_eq!(
        con.hget(arykey, &["filed2", "filed5"]),
        Ok((22.to_string(), 5.to_string()))
    );

    assert_eq!(con.hincr(arykey, "filed2", 4), Ok(26));
    assert_eq!(con.hincr(arykey, "filed5", 3.4), Ok(8.4));

    assert_eq!(con.hexists(arykey, "filed6"), Ok(0));
    assert_eq!(con.hset_nx(arykey, "filed6", 6), Ok(1));
    assert_eq!(con.hset_nx(arykey, "filed6", 6), Ok(0));

    assert_eq!(
        con.hvals(arykey),
        Ok((
            26.to_string(),
            3.to_string(),
            4.to_string(),
            8.4.to_string(),
            6.to_string()
        ))
    );

    assert_eq!(con.hset_nx("hashkey", "hashfiled6", 6), Ok(1));
    assert_eq!(con.hget("hashkey", "hashfiled6"), Ok(6.to_string()));

    let iter: redis::Iter<'_, (String, i64)> = con.hscan("hashkey").expect("hscan error");
    let mut found = HashSet::new();
    for item in iter {
        found.insert(item);
    }
    assert!(found.contains(&("hashfiled6".to_string(), 6)));

    let iter = con
        .hscan_match::<&str, &str, (String, i64)>(arykey, "filed6")
        .expect("hscan match error");
    let mut hscan_match_found = HashSet::new();
    for item in iter {
        hscan_match_found.insert(item);
    }
    assert!(hscan_match_found.contains(&("filed6".to_string(), 6)));
}

/// string基本操作:
/// set、append、setrange、getrange、getset、strlen
#[named]
#[test]
fn str_basic() {
    let arykey = function_name!();
    let mut con = get_conn(&RESTYPE.get_host());
    redis::cmd("DEL").arg(arykey).execute(&mut con);

    assert_eq!(con.set(arykey, "Hello World"), Ok("OK".to_string()));
    assert_eq!(con.setrange(arykey, 6, "Redis"), Ok(11));
    assert_eq!(con.getrange(arykey, 6, 10), Ok("Redis".to_string()));
    assert_eq!(
        con.getset(arykey, "Hello World"),
        Ok("Hello Redis".to_string())
    );
    assert_eq!(con.append(arykey, "!"), Ok(12));
    assert_eq!(con.strlen(arykey), Ok(12));
}

/// 单个long set基本操作, lsset, lsdump, lsput, lsgetall, lsdel, lslen, lsmexists, lsdset
#[named]
#[test]
fn test_lsset_basic() {
    let arykey = function_name!();
    let mut con = get_conn(&RESTYPE.get_host());
    redis::cmd("DEL").arg(arykey).execute(&mut con);

    // lsmalloc arykey 8, lsput argkey 1后的实际内存表现
    let lsset = vec![
        1u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ];
    redis::cmd("lsset")
        .arg(arykey)
        .arg(1)
        .arg(&lsset)
        .execute(&mut con);
    assert_eq!(
        redis::cmd("lsdump").arg(arykey).query(&mut con),
        Ok(lsset.clone())
    );

    assert_eq!(
        redis::cmd("lsput").arg(arykey).arg(2).query(&mut con),
        Ok(1)
    );
    assert_eq!(
        redis::cmd("lsgetall").arg(arykey).query(&mut con),
        Ok((1, 2))
    );

    assert_eq!(
        redis::cmd("lsdel").arg(arykey).arg(2).query(&mut con),
        Ok(1)
    );
    assert_eq!(redis::cmd("lslen").arg(arykey).query(&mut con), Ok(1));
    assert_eq!(
        redis::cmd("lsmexists")
            .arg(arykey)
            .arg(1)
            .arg(2)
            .query(&mut con),
        Ok("10".to_string())
    );

    let arykey = arykey.to_string() + "dset";
    redis::cmd("lsdset")
        .arg(&arykey)
        .arg(1)
        .arg(&lsset)
        .execute(&mut con);
    assert_eq!(
        redis::cmd("lsgetall").arg(&arykey).query(&mut con),
        Ok((1,))
    );
}

/// 单个zset基本操作:
/// zadd、zincrby、zrem、zremrangebyrank、zremrangebyscore、
/// zremrangebylex、zrevrange、zcard、zrange、zrangebyscore、
/// zrevrank、zrevrangebyscore、zrangebylex、zrevrangebylex、
/// zcount、zlexcount、zscore、zscan
#[named]
#[test]
fn zset_basic() {
    let arykey = function_name!();
    let mut con = get_conn(&RESTYPE.get_host());
    redis::cmd("DEL").arg(arykey).execute(&mut con);

    let values = &[
        (1, "one".to_string()),
        (2, "two".to_string()),
        (3, "three".to_string()),
        (4, "four".to_string()),
    ];

    assert_eq!(con.zadd_multiple(arykey, values), Ok(4));
    assert_eq!(
        con.zrange_withscores(arykey, 0, -1),
        Ok(vec![
            ("one".to_string(), 1),
            ("two".to_string(), 2),
            ("three".to_string(), 3),
            ("four".to_string(), 4),
        ])
    );

    assert_eq!(
        con.zrevrange_withscores(arykey, 0, -1),
        Ok(vec![
            ("four".to_string(), 4),
            ("three".to_string(), 3),
            ("two".to_string(), 2),
            ("one".to_string(), 1),
        ])
    );

    assert_eq!(con.zincr(arykey, "one", 4), Ok("5".to_string()));
    assert_eq!(con.zrem(arykey, "four"), Ok(1));
    assert_eq!(con.zremrangebyrank(arykey, 0, 0), Ok(1));
    assert_eq!(con.zrembyscore(arykey, 1, 3), Ok(1));

    let samescore = &[
        (0, "aaaa".to_string()),
        (0, "b".to_string()),
        (0, "c".to_string()),
        (0, "d".to_string()),
        (0, "e".to_string()),
    ];

    assert_eq!(con.zadd_multiple(arykey, samescore), Ok(5));
    assert_eq!(con.zrembylex(arykey, "[b", "(c"), Ok(1));
    assert_eq!(
        con.zrangebylex(arykey, "-", "(c"),
        Ok(vec!["aaaa".to_string(),])
    );
    assert_eq!(
        con.zrevrangebylex(arykey, "(c", "-"),
        Ok(vec!["aaaa".to_string(),])
    );
    assert_eq!(con.zcount(arykey, 0, 2), Ok(4));
    assert_eq!(con.zlexcount(arykey, "-", "+"), Ok(5));
    redis::cmd("DEL").arg(arykey).execute(&mut con);
    assert_eq!(con.zadd_multiple(arykey, values), Ok(4));

    assert_eq!(
        con.zrangebyscore(arykey, 0, 5),
        Ok(vec![
            "one".to_string(),
            "two".to_string(),
            "three".to_string(),
            "four".to_string(),
        ])
    );

    assert_eq!(
        con.zrevrangebyscore(arykey, 5, 0),
        Ok(vec![
            "four".to_string(),
            "three".to_string(),
            "two".to_string(),
            "one".to_string(),
        ])
    );

    assert_eq!(con.zcard(arykey), Ok(4));
    assert_eq!(con.zrank(arykey, "one"), Ok(0));
    assert_eq!(con.zscore(arykey, "one"), Ok(1));

    redis::cmd("DEL").arg(arykey).execute(&mut con);
    let values = &[(1, "one".to_string()), (2, "two".to_string())];
    assert_eq!(con.zadd_multiple(arykey, values), Ok(2));

    let res: Result<(i32, Vec<String>), RedisError> =
        redis::cmd("ZSCAN").arg(arykey).arg(0).query(&mut con);
    assert!(res.is_ok());
    let (cur, mut s): (i32, Vec<String>) = res.expect("ok");

    s.sort_unstable();
    assert_eq!(cur, 0i32);
    assert_eq!(s.len(), 4);
    assert_eq!(
        &s,
        &[
            "1".to_string(),
            "2".to_string(),
            "one".to_string(),
            "two".to_string()
        ]
    );
}

/// set基本操作:
/// sadd、smembers、srem、sismember、scard、spop、sscan
#[named]
#[test]
fn set_basic() {
    let arykey = function_name!();
    let mut con = get_conn(&RESTYPE.get_host());
    redis::cmd("DEL").arg(arykey).execute(&mut con);

    assert_eq!(con.sadd(arykey, "one"), Ok(1));
    assert_eq!(con.sadd(arykey, "two"), Ok(1));

    let res: Result<Vec<String>, RedisError> = con.smembers(arykey);
    assert!(res.is_ok());
    assert_eq!(res.expect("ok").len(), 2);

    assert_eq!(con.srem(arykey, "one"), Ok(1));
    assert_eq!(con.sismember(arykey, "one"), Ok(0));

    assert_eq!(con.scard(arykey), Ok(1));

    assert_eq!(con.srandmember(arykey), Ok("two".to_string()));
    assert_eq!(con.spop(arykey), Ok("two".to_string()));

    assert_eq!(con.sadd(arykey, "hello"), Ok(1));
    assert_eq!(con.sadd(arykey, "hi"), Ok(1));

    redis::cmd("DEL").arg("foo").execute(&mut con);
    assert_eq!(con.sadd("foo", &[1, 2, 3]), Ok(3));
    let res: Result<(i32, Vec<i32>), RedisError> =
        redis::cmd("SSCAN").arg("foo").arg(0).query(&mut con);
    assert!(res.is_ok());
    let (cur, mut s): (i32, Vec<i32>) = res.expect("ok");

    s.sort_unstable();
    assert_eq!(cur, 0i32);
    assert_eq!(s.len(), 3);
    assert_eq!(&s, &[1, 2, 3]);
}

/// Bitmap基本操作:
/// setbit、getbit、bitcount、bitpos、bitfield
#[named]
#[test]
fn bit_basic() {
    let arykey = function_name!();
    let mut con = get_conn(&RESTYPE.get_host());
    redis::cmd("DEL").arg(arykey).execute(&mut con);

    assert_eq!(con.setbit(arykey, 10086, true), Ok(0));
    assert_eq!(con.getbit(arykey, 10086), Ok(1));
    assert_eq!(con.bitcount(arykey), Ok(1));

    let res: Result<u64, RedisError> = redis::cmd("BITPOS")
        .arg(arykey)
        .arg(1)
        .arg(0)
        .query(&mut con);
    assert!(res.is_ok());
    assert_eq!(res.expect("ok"), 10086);

    let res: Result<Vec<u8>, RedisError> = redis::cmd("BITFIELD")
        .arg(arykey)
        .arg("GET")
        .arg("u4")
        .arg("0")
        .query(&mut con);
    assert!(res.is_ok());
    assert_eq!(res.expect("ok"), &[0u8]);
}
