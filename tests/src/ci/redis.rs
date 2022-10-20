/// # 已测试场景
/// - set 1 1, ..., set 10000 10000等一万个key已由java sdk预先写入,
/// 从mesh读取, 验证业务写入与mesh读取之间的一致性
/// - basic set
/// - value大小数组[4, 40, 400, 4000, 8000, 20000, 3000000],依次set后随机set,验证buffer扩容
/// - basic del
/// - basic incr
use std::io::{Error, ErrorKind, Result};

use crate::ci::env::Mesh;
use crate::redis_helper::*;

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

/// set 1 1, ..., set 10000 10000等一万个key已由java sdk预先写入,
/// 从mesh读取, 验证业务写入与mesh读取之间的一致性
#[test]
fn test_get_write_by_sdk() {
    let mut con = get_conn(&file!().get_host());
    for i in 1..=5 {
        assert_eq!(redis::cmd("GET").arg(i).query(&mut con), Ok(i));
    }
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

///依次set [4, 40, 400, 4000, 8000, 20000, 3000000]大小的value
///验证buffer扩容,buffer初始容量4K,扩容每次扩容两倍
///后将[4, 40, 400, 4000, 8000, 20000, 3000000] shuffle后再依次set
///测试步骤:
///  1. set, key value size 4k以下，4次
///  3. set key value size 4k~8k，一次, buffer由4k扩容到8k
///  4. set key value size 8k~16k，一次，buffer在一次请求中扩容两次，由8k扩容到16k，16k扩容到32k，
///  5. set, key value size 2M以上，1次
///  6. 以上set请求乱序set一遍
#[test]
fn test_set_value_fix_size() {
    let mut con = get_conn(&file!().get_host());

    let mut v_sizes = [4, 40, 400, 4000, 8000, 20000, 3000000];
    for v_size in v_sizes {
        let val = vec![1u8; v_size];
        redis::cmd("SET")
            .arg("bar_set_value_size")
            .arg(&val)
            .execute(&mut con);
        assert_eq!(
            redis::cmd("GET").arg("bar_set_value_size").query(&mut con),
            Ok(val)
        );
    }

    //todo random iter
    use rand::seq::SliceRandom;
    let mut rng = rand::thread_rng();
    v_sizes.shuffle(&mut rng);
    for v_size in v_sizes {
        let val = vec![1u8; v_size];
        redis::cmd("SET")
            .arg("bar_set_value_size")
            .arg(&val)
            .execute(&mut con);
        assert_eq!(
            redis::cmd("GET").arg("bar_set_value_size").query(&mut con),
            Ok(val)
        );
    }
}

#[test]
fn test_basic_del() {
    let mut con = get_conn(&file!().get_host());

    redis::cmd("SET").arg("foodel").arg(42).execute(&mut con);

    assert_eq!(redis::cmd("GET").arg("foodel").query(&mut con), Ok(42));

    redis::cmd("DEL").arg("foodel").execute(&mut con);

    assert_eq!(
        redis::cmd("GET").arg("foodel").query(&mut con),
        Ok(None::<usize>)
    );
}

#[test]
fn test_basic_incr() {
    let mut con = get_conn(&file!().get_host());

    redis::cmd("SET").arg("fooincr").arg(42).execute(&mut con);
    assert_eq!(
        redis::cmd("INCR").arg("fooincr").query(&mut con),
        Ok(43usize)
    );
}
