//! 各种数据结构的测试
use crate::ci::env::*;
use crate::redis::RESTYPE;
use crate::redis_helper::*;
use function_name::named;
use redis::{Commands, Connection, RedisError};
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
    let argkey = function_name!();
    let mut con = get_conn(&RESTYPE.get_host());
    redis::cmd("DEL").arg(argkey).execute(&mut con);

    assert_eq!(con.lpush(argkey, &[1, 2]), Ok(2));
    assert_eq!(con.lpush(argkey, &[3, 4]), Ok(4));
    assert_eq!(con.rpush(argkey, &[5, 6]), Ok(6));
    assert_eq!(con.rpush(argkey, &[7, 8]), Ok(8));

    assert_eq!(con.lrange(argkey, 0, -1), Ok((4, 3, 2, 1, 5, 6, 7, 8)));

    assert_eq!(con.lpop(argkey, Default::default()), Ok(4));
    assert_eq!(con.rpop(argkey, Default::default()), Ok(8));
    assert_eq!(con.llen(argkey), Ok(6));
    assert_eq!(con.lrange(argkey, 0, -1), Ok((3, 2, 1, 5, 6, 7)));

    assert_eq!(con.lset(argkey, 0, 4), Ok(true));
    assert_eq!(con.lindex(argkey, 0), Ok(4));

    assert_eq!(con.linsert_before(argkey, 4, 4), Ok(7));
    assert_eq!(con.linsert_after(argkey, 7, 7), Ok(8));
    assert_eq!(con.lrange(argkey, 0, -1), Ok((4, 4, 2, 1, 5, 6, 7, 7)));

    assert_eq!(con.lrem(argkey, 1, 4), Ok(1));
    assert_eq!(con.lrem(argkey, -2, 7), Ok(2));
    assert_eq!(con.lrem(argkey, 0, 2), Ok(1));
    assert_eq!(con.lrange(argkey, 0, -1), Ok((4, 1, 5, 6)));

    assert_eq!(con.ltrim(argkey, 1, 2), Ok(true));
    assert_eq!(con.lpush_exists(argkey, 1), Ok(3));
    assert_eq!(con.rpush_exists(argkey, 5), Ok(4));
    assert_eq!(con.lrange(argkey, 0, -1), Ok((1, 1, 5, 5)));
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
    let argkey = function_name!();
    let mut con = get_conn(&RESTYPE.get_host());
    redis::cmd("DEL").arg(argkey).execute(&mut con);

    assert_eq!(
        redis::cmd("GEOADD")
            .arg(argkey)
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
            .arg(argkey)
            .arg("Beijing")
            .arg("Tianjin")
            .arg("km")
            .query(&mut con),
        Ok(489.9349)
    );

    assert_eq!(
        redis::cmd("GEOPOS")
            .arg(argkey)
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
            .arg(argkey)
            .arg("Beijing")
            .arg("Tianjin")
            .query(&mut con),
        Ok(("u8vk6wjr4e0".to_string(), "u9e5nqkuc90".to_string()))
    );
    // operation not permitted on a read only server
    // assert_eq!(
    //     redis::cmd("GEORADIUS")
    //         .arg(argkey)
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
    //         .arg(argkey)
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
    let argkey = function_name!();
    let mut con = get_conn(&RESTYPE.get_host());
    redis::cmd("DEL").arg(argkey).execute(&mut con);
    redis::cmd("DEL").arg("hashkey").execute(&mut con);

    assert_eq!(
        con.hset_multiple(
            argkey,
            &[("filed1", 1), ("filed2", 2), ("filed3", 3), ("filed4", 4),]
        ),
        Ok(true)
    );
    assert_eq!(
        con.hgetall(argkey),
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

    assert_eq!(con.hdel(argkey, "filed1"), Ok(1));
    assert_eq!(con.hlen(argkey), Ok(3));
    assert_eq!(
        con.hkeys(argkey),
        Ok((
            "filed2".to_string(),
            "filed3".to_string(),
            "filed4".to_string(),
        ))
    );

    assert_eq!(con.hset(argkey, "filed5", 5), Ok(1));
    assert_eq!(con.hset(argkey, "filed2", 22), Ok(0));
    assert_eq!(
        con.hget(argkey, &["filed2", "filed5"]),
        Ok((22.to_string(), 5.to_string()))
    );

    assert_eq!(con.hincr(argkey, "filed2", 4), Ok(26));
    assert_eq!(con.hincr(argkey, "filed5", 3.4), Ok(8.4));

    assert_eq!(con.hexists(argkey, "filed6"), Ok(0));
    assert_eq!(con.hset_nx(argkey, "filed6", 6), Ok(1));
    assert_eq!(con.hset_nx(argkey, "filed6", 6), Ok(0));

    assert_eq!(
        con.hvals(argkey),
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
        .hscan_match::<&str, &str, (String, i64)>(argkey, "filed6")
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
    let argkey = function_name!();
    let mut con = get_conn(&RESTYPE.get_host());
    redis::cmd("DEL").arg(argkey).execute(&mut con);

    assert_eq!(con.set(argkey, "Hello World"), Ok("OK".to_string()));
    assert_eq!(con.setrange(argkey, 6, "Redis"), Ok(11));
    assert_eq!(con.getrange(argkey, 6, 10), Ok("Redis".to_string()));
    assert_eq!(
        con.getset(argkey, "Hello World"),
        Ok("Hello Redis".to_string())
    );
    assert_eq!(con.append(argkey, "!"), Ok(12));
    assert_eq!(con.strlen(argkey), Ok(12));
}

/// 单个long set基本操作, lsset, lsdump, lsput, lsgetall, lsdel, lslen, lsmexists, lsdset
#[named]
#[test]
fn test_lsset_basic() {
    let argkey = function_name!();
    let mut con = get_conn(&RESTYPE.get_host());
    redis::cmd("DEL").arg(argkey).execute(&mut con);

    // lsmalloc argkey 8, lsput argkey 1后的实际内存表现
    let lsset = vec![
        1u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ];
    redis::cmd("lsset")
        .arg(argkey)
        .arg(1)
        .arg(&lsset)
        .execute(&mut con);
    assert_eq!(
        redis::cmd("lsdump").arg(argkey).query(&mut con),
        Ok(lsset.clone())
    );

    assert_eq!(
        redis::cmd("lsput").arg(argkey).arg(2).query(&mut con),
        Ok(1)
    );
    assert_eq!(
        redis::cmd("lsgetall").arg(argkey).query(&mut con),
        Ok((1, 2))
    );

    assert_eq!(
        redis::cmd("lsdel").arg(argkey).arg(2).query(&mut con),
        Ok(1)
    );
    assert_eq!(redis::cmd("lslen").arg(argkey).query(&mut con), Ok(1));
    assert_eq!(
        redis::cmd("lsmexists")
            .arg(argkey)
            .arg(1)
            .arg(2)
            .query(&mut con),
        Ok("10".to_string())
    );

    let argkey = argkey.to_string() + "dset";
    redis::cmd("lsdset")
        .arg(&argkey)
        .arg(1)
        .arg(&lsset)
        .execute(&mut con);
    assert_eq!(
        redis::cmd("lsgetall").arg(&argkey).query(&mut con),
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
    let argkey = function_name!();
    let mut con = get_conn(&RESTYPE.get_host());
    redis::cmd("DEL").arg(argkey).execute(&mut con);

    let values = &[
        (1, "one".to_string()),
        (2, "two".to_string()),
        (3, "three".to_string()),
        (4, "four".to_string()),
    ];

    assert_eq!(con.zadd_multiple(argkey, values), Ok(4));
    assert_eq!(
        con.zrange_withscores(argkey, 0, -1),
        Ok(vec![
            ("one".to_string(), 1),
            ("two".to_string(), 2),
            ("three".to_string(), 3),
            ("four".to_string(), 4),
        ])
    );

    assert_eq!(
        con.zrevrange_withscores(argkey, 0, -1),
        Ok(vec![
            ("four".to_string(), 4),
            ("three".to_string(), 3),
            ("two".to_string(), 2),
            ("one".to_string(), 1),
        ])
    );

    assert_eq!(con.zincr(argkey, "one", 4), Ok("5".to_string()));
    assert_eq!(con.zrem(argkey, "four"), Ok(1));
    assert_eq!(con.zremrangebyrank(argkey, 0, 0), Ok(1));
    assert_eq!(con.zrembyscore(argkey, 1, 3), Ok(1));

    let samescore = &[
        (0, "aaaa".to_string()),
        (0, "b".to_string()),
        (0, "c".to_string()),
        (0, "d".to_string()),
        (0, "e".to_string()),
    ];

    assert_eq!(con.zadd_multiple(argkey, samescore), Ok(5));
    assert_eq!(con.zrembylex(argkey, "[b", "(c"), Ok(1));
    assert_eq!(
        con.zrangebylex(argkey, "-", "(c"),
        Ok(vec!["aaaa".to_string(),])
    );
    assert_eq!(
        con.zrevrangebylex(argkey, "(c", "-"),
        Ok(vec!["aaaa".to_string(),])
    );
    assert_eq!(con.zcount(argkey, 0, 2), Ok(4));
    assert_eq!(con.zlexcount(argkey, "-", "+"), Ok(5));
    redis::cmd("DEL").arg(argkey).execute(&mut con);
    assert_eq!(con.zadd_multiple(argkey, values), Ok(4));

    assert_eq!(
        con.zrangebyscore(argkey, 0, 5),
        Ok(vec![
            "one".to_string(),
            "two".to_string(),
            "three".to_string(),
            "four".to_string(),
        ])
    );

    assert_eq!(
        con.zrevrangebyscore(argkey, 5, 0),
        Ok(vec![
            "four".to_string(),
            "three".to_string(),
            "two".to_string(),
            "one".to_string(),
        ])
    );

    assert_eq!(con.zcard(argkey), Ok(4));
    assert_eq!(con.zrank(argkey, "one"), Ok(0));
    assert_eq!(con.zscore(argkey, "one"), Ok(1));

    redis::cmd("DEL").arg(argkey).execute(&mut con);
    let values = &[(1, "one".to_string()), (2, "two".to_string())];
    assert_eq!(con.zadd_multiple(argkey, values), Ok(2));

    let res: Result<(i32, Vec<String>), RedisError> =
        redis::cmd("ZSCAN").arg(argkey).arg(0).query(&mut con);
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
    let argkey = function_name!();
    let mut con = get_conn(&RESTYPE.get_host());
    redis::cmd("DEL").arg(argkey).execute(&mut con);

    assert_eq!(con.sadd(argkey, "one"), Ok(1));
    assert_eq!(con.sadd(argkey, "two"), Ok(1));

    let res: Result<Vec<String>, RedisError> = con.smembers(argkey);
    assert!(res.is_ok());
    assert_eq!(res.expect("ok").len(), 2);

    assert_eq!(con.srem(argkey, "one"), Ok(1));
    assert_eq!(con.sismember(argkey, "one"), Ok(0));

    assert_eq!(con.scard(argkey), Ok(1));

    assert_eq!(con.srandmember(argkey), Ok("two".to_string()));
    assert_eq!(con.spop(argkey), Ok("two".to_string()));

    assert_eq!(con.sadd(argkey, "hello"), Ok(1));
    assert_eq!(con.sadd(argkey, "hi"), Ok(1));

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
    let argkey = function_name!();
    let mut con = get_conn(&RESTYPE.get_host());
    redis::cmd("DEL").arg(argkey).execute(&mut con);

    assert_eq!(con.setbit(argkey, 10086, true), Ok(0));
    assert_eq!(con.getbit(argkey, 10086), Ok(1));
    assert_eq!(con.bitcount(argkey), Ok(1));

    let res: Result<u64, RedisError> = redis::cmd("BITPOS")
        .arg(argkey)
        .arg(1)
        .arg(0)
        .query(&mut con);
    assert!(res.is_ok());
    assert_eq!(res.expect("ok"), 10086);

    let res: Result<Vec<u8>, RedisError> = redis::cmd("BITFIELD")
        .arg(argkey)
        .arg("GET")
        .arg("u4")
        .arg("0")
        .query(&mut con);
    assert!(res.is_ok());
    assert_eq!(res.expect("ok"), &[0u8]);
}

#[test]
fn sdiff_cmds() {
    let skey1 = "test_set_key1";
    let skey2 = "test_set_key2";
    let skey3 = "test_set_key3";
    let sval1 = vec!["0", "1", "2"];
    let sval2 = vec!["1", "2", "3"];

    // prepare data
    // clear old data
    let mut conn = get_conn(&RESTYPE.get_host());
    redis::cmd("DEL").arg(skey1).arg(skey2).execute(&mut conn);

    // sadd values
    sadd(&mut conn, skey1, &sval1);
    sadd(&mut conn, skey2, &sval2);

    // sdiff with hashkey
    redis::cmd("HASHKEY").arg(skey1).execute(&mut conn);
    let diff_vals = redis::cmd("SDIFF")
        .arg(skey1)
        .arg(skey2)
        .query::<Vec<String>>(&mut conn)
        .unwrap();
    let diff_vals_real = vec!["0".to_string()];
    assert_eq!(diff_vals, diff_vals_real);

    // sdiffstore with hashkey
    redis::cmd("HASHKEY").arg(skey1).execute(&mut conn);
    let diff_count = redis::cmd("SDIFFSTORE")
        .arg(skey3)
        .arg(skey1)
        .arg(skey2)
        .query::<usize>(&mut conn)
        .unwrap();
    assert_eq!(diff_count, diff_vals.len(), "sdiffstore failed");

    // sdiff without hashkeyq
    let should_err = redis::cmd("SDIFF")
        .arg(skey1)
        .arg(skey2)
        .query::<Vec<String>>(&mut conn)
        .is_err();
    assert_eq!(should_err, true, "sdiff without hashkeyq will failed");
}

#[test]
fn sinter_cmds() {
    let skey1 = "test_set_key1";
    let skey2 = "test_set_key2";
    let skey3 = "test_set_key3";
    let sval1 = vec!["0", "1", "2"];
    let sval2 = vec!["1", "2", "3"];

    // prepare data
    // clear old data
    let mut conn = get_conn(&RESTYPE.get_host());
    redis::cmd("DEL").arg(skey1).arg(skey2).execute(&mut conn);

    // sadd values
    sadd(&mut conn, skey1, &sval1);
    sadd(&mut conn, skey2, &sval2);

    // sinter with hashkey
    redis::cmd("HASHKEY").arg(skey1).execute(&mut conn);
    let sinter_vals = redis::cmd("SINTER")
        .arg(skey1)
        .arg(skey2)
        .query::<Vec<String>>(&mut conn)
        .unwrap();
    let sinter_vals_real = vec!["1".to_string(), "2".to_string()];
    assert_eq!(sinter_vals, sinter_vals_real);

    // sinterstore with hashkey
    redis::cmd("HASHKEY").arg(skey1).execute(&mut conn);
    let inter_count = redis::cmd("SINTERSTORE")
        .arg(skey3)
        .arg(skey1)
        .arg(skey2)
        .query::<usize>(&mut conn)
        .unwrap();
    assert_eq!(inter_count, sinter_vals.len(), "sunionstore failed");

    // sinter without hashkeyq
    let should_err = redis::cmd("SINTER")
        .arg(skey3)
        .arg(skey1)
        .arg(skey2)
        .query::<Vec<String>>(&mut conn)
        .is_err();
    assert_eq!(should_err, true, "sinter without hashkeyq will failed");
}

#[test]
fn sunion_cmds() {
    let skey1 = "test_set_key1";
    let skey2 = "test_set_key2";
    let skey3 = "test_set_key3";
    let sval1 = vec!["0", "1", "2"];
    let sval2 = vec!["1", "2", "3"];

    // prepare data
    // clear old data
    let mut conn = get_conn(&RESTYPE.get_host());
    redis::cmd("DEL").arg(skey1).arg(skey2).execute(&mut conn);

    // sadd values
    sadd(&mut conn, skey1, &sval1);
    sadd(&mut conn, skey2, &sval2);

    // sunion with hashkey
    redis::cmd("HASHKEY").arg(skey1).execute(&mut conn);
    let sunion_vals = redis::cmd("SUNION")
        .arg(skey1)
        .arg(skey2)
        .query::<Vec<String>>(&mut conn)
        .unwrap();
    let sunion_vals_real = vec![
        "0".to_string(),
        "1".to_string(),
        "2".to_string(),
        "3".to_string(),
    ];
    assert_eq!(sunion_vals, sunion_vals_real);

    // sunionstore with hashkey
    redis::cmd("HASHKEY").arg(skey1).execute(&mut conn);
    let inter_count = redis::cmd("SUNIONSTORE")
        .arg(skey3)
        .arg(skey1)
        .arg(skey2)
        .query::<usize>(&mut conn)
        .unwrap();
    assert_eq!(inter_count, sunion_vals.len(), "sunionstore failed");

    // sunion without hashkeyq
    let should_err = redis::cmd("SUNION")
        .arg(skey3)
        .arg(skey1)
        .arg(skey2)
        .query::<Vec<String>>(&mut conn)
        .is_err();
    assert!(should_err, "sdiff without hashkeyq will failed");
}

fn sadd(conn: &mut Connection, key: &str, members: &Vec<&str>) {
    let mut cmd_add = redis::cmd("SADD");
    let mut cmd_add = cmd_add.arg(key);
    for v in members.iter() {
        cmd_add = cmd_add.arg(v);
    }
    cmd_add.execute(conn);
}
