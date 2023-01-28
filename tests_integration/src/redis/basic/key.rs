//! 针对key相关基本测试，如set，get，过期等
use crate::ci::env::*;
use crate::redis::RESTYPE;
use crate::redis_helper::*;
use chrono::prelude::*;
use function_name::named;
use redis::Commands;
/// 基本set场景，key固定为foo或bar，value为简单数字或字符串
/// set ex
#[test]
#[named]
fn test_basic_set() {
    let argkey = function_name!();
    let mut con = get_conn(&RESTYPE.get_host());

    redis::cmd("SET").arg("fooset").arg(42).execute(&mut con);
    assert_eq!(redis::cmd("GET").arg("fooset").query(&mut con), Ok(42));

    redis::cmd("SET").arg("barset").arg("foo").execute(&mut con);
    assert_eq!(
        redis::cmd("GET").arg("barset").query(&mut con),
        Ok(b"foo".to_vec())
    );

    redis::cmd("SET")
        .arg(argkey)
        .arg(argkey)
        .arg("EX")
        .arg(3)
        .execute(&mut con);
    assert!(con.ttl::<&str, isize>(argkey).unwrap() >= 0);
}

/// mget 两个key, 其中只有一个set了, 预期应有一个none结果
#[named]
#[test]
fn test_set_optionals() {
    let argkey = function_name!();
    let mut con = get_conn(&RESTYPE.get_host());

    redis::cmd("SET").arg(argkey).arg(1).execute(&mut con);

    let (a, b): (Option<i32>, Option<i32>) = redis::cmd("MGET")
        .arg(argkey)
        .arg("missing")
        .query(&mut con)
        .unwrap();
    assert_eq!(a, Some(1i32));
    assert_eq!(b, None);
}

#[named]
#[test]
fn test_basic_del() {
    let argkey = function_name!();
    let mut con = get_conn(&RESTYPE.get_host());

    redis::cmd("SET").arg(argkey).arg(42).execute(&mut con);

    assert_eq!(redis::cmd("GET").arg(argkey).query(&mut con), Ok(42));

    redis::cmd("DEL").arg(argkey).execute(&mut con);

    assert_eq!(
        redis::cmd("GET").arg(argkey).query(&mut con),
        Ok(None::<usize>)
    );
}

#[named]
#[test]
fn test_basic_incr() {
    let argkey = function_name!();
    let mut con = get_conn(&RESTYPE.get_host());

    redis::cmd("SET").arg(argkey).arg(42).execute(&mut con);
    assert_eq!(redis::cmd("INCR").arg(argkey).query(&mut con), Ok(43usize));
}

//getset key不存在时 返回nil 并set key
//get key =2
//getset key 1 返回旧值2
//get key =1
#[named]
#[test]
fn getset_basic() {
    let argkey = function_name!();
    let mut con = get_conn(&RESTYPE.get_host());
    redis::cmd("DEL").arg(argkey).execute(&mut con);

    assert_eq!(
        con.getset::<&str, i32, Option<i32>>(argkey, 2)
            .map_err(|e| panic!("set error:{:?}", e))
            .expect("set err"),
        None
    );
    assert_eq!(con.get(argkey), Ok(2));
    assert_eq!(con.getset(argkey, 1), Ok(2));
    assert_eq!(con.get(argkey), Ok(1));
}

/// - mset ("xinxinkey1", 1), ("xinxinkey2", 2), ("xinxinkey3", 3),(argkey, 4)
/// - expire_at 设置xinxinkey1的过期时间为当前秒级别时间戳+2s
/// - 判断是否剩余过期过期时间，并get
/// -  setex key2 4 22 将key2 value 改成22并设置过期时间4s
/// - ttl/pttl key3存在 但是没有设置过期时间=》-1
/// - ttl key1没过期
/// - sleep 2s
/// - pttl key2 没过期
/// - get value为22
/// - exists 检查key1不存在 因为已经过期 =》0
/// - ttl/pttl key1不存在时=》-2
/// - setnx key1 2 =>1 key1不存在（因为已经过期被删掉）才能setnx成功
/// - get key1 =>11
/// - setnx 已经存在的key3 =>0
/// - expire key3过期时间为1s
/// - incrby 4 =>8
/// - decrby 2 =>6
/// - decr =>5
/// - incrbyfloat 4.4 =>9.4
/// - pexpireat 设置argkey的过期时间为当前时间戳+2000ms p都是ms级别
/// - sleep 1s
/// - pexpire 设置argkey过期时间为5000ms
/// - ttl argkey过期时间》2000 由于pexpire把key3过期时间覆盖
/// - exists key3 0 已经过期
/// - persist argkey 移除过期时间
/// - ttl argkey -1 已经移除
#[named]
#[test]
fn string_basic() {
    let argkey = function_name!();
    let mut con = get_conn(&RESTYPE.get_host());
    redis::cmd("DEL").arg(argkey).execute(&mut con);
    redis::cmd("DEL").arg("xinxinkey1").execute(&mut con);
    redis::cmd("DEL").arg("xinxinkey2").execute(&mut con);
    redis::cmd("DEL").arg("xinxinkey3").execute(&mut con);
    assert_eq!(
        con.set_multiple(&[
            ("xinxinkey1", 1),
            ("xinxinkey2", 2),
            ("xinxinkey3", 3),
            (argkey, 4)
        ]),
        Ok(true)
    );
    loop {
        let now = Local::now();
        assert_eq!(
            con.expire_at("xinxinkey1", (now.timestamp() + 2) as usize),
            Ok(1)
        );
        let last_time: i128 = con
            .ttl("xinxinkey1")
            .map_err(|e| panic!("ttl error:{:?}", e))
            .expect("ttl err");
        if last_time > 0 {
            assert_eq!(con.exists("xinxinkey1"), Ok(1));
            break;
        } else if last_time >= -1 {
            assert_eq!(con.get("xinxinkey1"), Ok(1));
        } else {
            assert_eq!(con.get::<&str, Option<i32>>("xinxinkey1"), Ok(None));

            assert_eq!(con.ttl("xinxinkey1"), Ok(-2));
            assert_eq!(con.pttl("xinxinkey1"), Ok(-2));
            assert_eq!(con.set_nx("xinxinkey1", 1), Ok(1));
            assert_eq!(con.get("xinxinkey1"), Ok(1));
        }
    }

    loop {
        assert_eq!(con.set_ex("xinxinkey2", 22, 3), Ok(true));
        let plast_time: i128 = con
            .pttl("xinxinkey2")
            .map_err(|e| panic!("pttl error:{:?}", e))
            .expect("pttl err");
        if plast_time > 0 {
            assert_eq!(con.get("xinxinkey2"), Ok(22));
            break;
        } else if plast_time >= -1 {
            assert_eq!(con.get("xinxinkey2"), Ok(22))
        } else {
            assert_eq!(con.get::<&str, Option<i32>>("xinxinkey2"), Ok(None))
        }
    }

    assert_eq!(con.ttl("xinxinkey3"), Ok(-1));
    assert_eq!(con.pttl("xinxinkey3"), Ok(-1));
    assert_eq!(con.set_nx("xinxinkey3", 2), Ok(0));

    assert_eq!(con.expire("xinxinkey3", 1), Ok(1));

    assert_eq!(con.incr(argkey, 4), Ok(8));
    assert_eq!(con.decr(argkey, 2), Ok(6));
    assert_eq!(redis::cmd("DECR").arg(argkey).query(&mut con), Ok(5));
    assert_eq!(con.incr(argkey, 4.4), Ok(9.4));

    loop {
        assert_eq!(
            con.pexpire_at(argkey, (Local::now().timestamp_millis() + 2000) as usize),
            Ok(1)
        );
        assert_eq!(con.pexpire(argkey, 4000 as usize), Ok(1));
        let cov_plast_time: i128 = con
            .pttl(argkey)
            .map_err(|e| panic!("pttl error:{:?}", e))
            .expect("pttl err");
        //println!("{:?}", cov_plast_time);
        if cov_plast_time > 2000 {
            assert_eq!(con.persist(argkey), Ok(1));

            assert_eq!(con.ttl(argkey), Ok(-1));
            break;
        } else if cov_plast_time >= -1 {
            assert_eq!(con.get(argkey), Ok(9.4));
        } else {
            assert_eq!(con.get::<&str, Option<i32>>(argkey), Ok(None))
        }
    }
}
