//! # 已测试场景
//! ## 基本操作验证
//! - get set incr incrby del mincr decr
//! - mget 1w个key 少量key
//! # 异常场景
//! - max-diff 当下一个setkey 超过当前最大key 提示too big
//! - key 类型错误（非long/没加配置列） 提示invaild key
//!    配置列错误 提示配置列错误
//! ## 复杂场景
//!  - set 1 1, ..., set 10000 10000等一万个key已由java sdk预先写入,
//! 从mesh读取, 验证业务写入与mesh读取之间的一致性
use crate::ci::env::Mesh;
use ahash::{HashMap, HashMapExt};
use assert_panic::assert_panic;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use redis::{Client, Commands, Connection};
use std::vec;

use crate::ci::env::exists_key_iter;

#[allow(dead_code)]
fn rand_num() -> u32 {
    let mut rng = rand::thread_rng();
    rng.gen::<u32>()
    //rng.gen_range(0..18446744073709551615)
    //18446744073709551
}
#[allow(dead_code)]
fn rand_key(tail: &str) -> String {
    let mut rng = thread_rng();
    let mut s: String = (&mut rng)
        .sample_iter(Alphanumeric)
        .take(5)
        .map(char::from)
        .collect();
    s.push_str(tail);
    s
}
#[allow(dead_code)]
fn record_max_key() {}

// set 1 2, ..., set 10000 10000等一万个key已由java sdk预先写入,
// set 1.repost 1 set 1.comment 1 . set 1.like.1
// 从mesh读取, 验证业务写入与mesh读取之间的一致性
#[test]
fn test_consum_diff_write() {
    // 单独get 耗时长 验证通过
    // let column_cfg = vec![".repost", ".comment", ".like"];
    // for column in column_cfg.iter() {
    //     for i in exists_key_iter() {
    //         let mut key = i.to_string();
    //         key.push_str(column);
    //         println!("{:?}", key);
    //         assert_eq!(redis::cmd("GET").arg(key).query(&mut get_conn()), Ok(i));
    //     }
    // }
    //get 整个key
    for i in exists_key_iter() {
        let value = i.to_string();
        let all_value = format!("repost:{},like:{},comment:{}", value, value, value);
        assert_eq!(
            redis::cmd("GET").arg(i).query(&mut get_conn()),
            Ok(all_value)
        );
    }
}
// 测试场景1 key为long类型并且带配置列
// 特征：key为纯数字long类型 key指定为44
// 测试端口配置了三列 repost comment like
// set 44.repost 20 , set 44.comment 20   , set 44.like 20

//过程：建立连接
//轮询向指定列里发送 value为20 的key
//再get 做断言 判断get到的val和set进去的val是否相等
#[test]
fn test_basic_set() {
    test_set_key_value(44, 44);
}

// 测试场景2：在测试1的基础上 让下一个key的大小比当前已知最大key大90000 和大10000000000（1e10）
// 已知配置中max-diff为1000000000 （1e9）
// 对该key的数据类型范围进行限制之外，还需要检测该key减去所要存储的table中的当前现有最大key的差值，这个差值不应超过max-diff
// 特征:已知当前max-key为10000 现在set  key1=max-key+90000    key2=max-key+1e10 value为20
// key1:set 100000.like 20    key2:set 4821779284223285.like 20

//流程
// 分别向like列 发送value为20的key1和key2
// 在get key1是否为20  如果key2返回key too big 测试通过

#[test]
fn test_key_maxdiff_set() {
    let value = 20;
    let key1 = "100000.like";
    let key2 = "4821779284223285.like";

    let _: () = get_conn()
        .set(key1, value)
        .map_err(|e| panic!("set error:{:?}", e))
        .expect("set err");
    assert_eq!(
        redis::cmd("GET").arg(key1).query(&mut get_conn()),
        Ok(value)
    );

    assert_panic!(panic!( "{:?}", get_conn().set::<String, u8, String>(key2.to_string(), value)), String, contains "key too big");
}

// 测试场景3 key的异常case
// key为 long类型但是不带配置列  提示Invalid key
// key为long类型并且配置列错误  提示No schema
// key为string类型非long导致的异常  提示nvalid key
// 异常case eg:  set 44 20 , set 44.unlike 30  set like 40
#[test]
fn test_diffkey_set() {
    let value = 20;

    let key_nocolumn = 44;
    assert_panic!(panic!( "{:?}", get_conn().set::<u8, u8, String>(key_nocolumn, value)), String, contains "Invalid key");

    let key_errcolumn = "44.unlike";
    assert_panic!(panic!( "{:?}", get_conn().set::<String, u8, String>(key_errcolumn.to_string(), value)), String, contains "No schema");

    let key_string = "like";
    assert_panic!(panic!( "{:?}", get_conn().set::<String, u8, String>(key_string.to_string(), value)), String, contains "Invalid key");
}

//流程
//先对每一列set value为20 key为64
//再get key.column
// get key的指定列=>获取一个value
// get 64.like => "30"

//再get key
// get key=> 得到所有列的值
// get 64 =>"repost:30,comment:30,like:30"
#[test]
fn test_basic_get() {
    test_set_key_value(6666666, 64);

    assert_eq!(
        redis::cmd("GET").arg(6666666).query(&mut get_conn()),
        Ok(String::from("repost:64,like:64,comment:64"))
    );
}

// 测试场景 单独删除某一列 get到的value为0
//如果删除整个key get的结果是"“ 。get某一列结果是nil 相当于把key+列删除
//单独删除某一列的key del 1234567.like
//删除整个key del 1234567

//流程 1. 先检查get key是否存在
// 2. set 每一列
// 3. get 每一列验证是否set成功
// 4. del 每一列后再get是否删除成功 del 1234567.like
// 5. del整个key  验证结果是否为"repost:0,like:0,comment:0"
// 6.del 1234567 验证结果是否为“”
// 7. get key。repost 验证结果是否为nil
#[test]
fn test_basic_del() {
    let key = 123456789;
    let value = 456;
    assert_eq!(
        redis::cmd("GET").arg(key).query(&mut get_conn()),
        Ok("".to_string())
    );

    let column_cfg = vec![".repost", ".comment", ".like"];
    for column in column_cfg.iter() {
        let mut key_column = key.to_string();
        key_column.push_str(column);

        let _: () = get_conn()
            .set(&key_column, value)
            .map_err(|e| panic!("set error:{:?}", e))
            .expect("set err");

        assert_eq!(
            redis::cmd("GET").arg(&key_column).query(&mut get_conn()),
            Ok(value)
        );

        assert_eq!(
            redis::cmd("DEL").arg(&key_column).query(&mut get_conn()),
            Ok(1)
        );
        assert_eq!(
            redis::cmd("GET").arg(&key_column).query(&mut get_conn()),
            Ok(0)
        );
    }

    assert_eq!(
        redis::cmd("GET").arg(key).query(&mut get_conn()),
        Ok(String::from("repost:0,like:0,comment:0"))
    );

    assert_eq!(redis::cmd("DEL").arg(&key).query(&mut get_conn()), Ok(1));
    assert_eq!(
        redis::cmd("GET").arg(key).query(&mut get_conn()),
        Ok("".to_string())
    );

    let mut stringkey = key.to_string();
    stringkey.push_str(".repost");

    assert_eq!(
        redis::cmd("GET").arg(stringkey).query(&mut get_conn()),
        Ok(None::<usize>)
    );
}

//测试场景：incrBY只能incrby 指定key的指定列
//单独incrby某一列 get到的value为value+incr_num  incr 223456789.repost 2

//流程
// 1. set 每一列
// 2. get 每一列验证是否set成功
// 3. incrby 每一列后再get是否incr 成功 incrby 223456789.repost 2
//  incr后的值为value+incr_num
//4. incr整个key incr 223456789 报 Invalid key

#[test]
fn test_basic_incrby() {
    let key: u32 = 223456789;
    let value = 456;
    let incr_num = 2;

    let column_cfg = vec![".repost", ".comment", ".like"];
    for column in column_cfg.iter() {
        let mut key_column = key.to_string();
        key_column.push_str(column);

        let _: () = get_conn()
            .set(&key_column, value)
            .map_err(|e| panic!("set error:{:?}", e))
            .expect("set err");
        assert_eq!(
            redis::cmd("GET").arg(&key_column).query(&mut get_conn()),
            Ok(value)
        );

        assert_eq!(
            redis::cmd("INCRBY")
                .arg(&key_column)
                .arg(incr_num)
                .query(&mut get_conn()),
            Ok(value + incr_num)
        );
        assert_eq!(
            redis::cmd("GET").arg(&key_column).query(&mut get_conn()),
            Ok(value + incr_num)
        );
    }

    assert_panic!(panic!( "{:?}", get_conn().incr::<u32, u32, String>(key, 2)), String, contains "Invalid key");
}

//set 111111 222222
//mincr key1 key2
// mget key1 key2
#[test]
fn test_basic_mincr() {
    let v1 = 1111111;
    let v2 = 2222222;
    redis::cmd("DEL").arg(v1).arg(v2).execute(&mut get_conn());

    test_set_key_value(v1, v1 as i64);
    test_set_key_value(v2, v2 as i64);

    let column_cfg = vec![".repost", ".comment", ".like"];
    let mut mincr_keys = vec![];
    for column in column_cfg.iter() {
        let mut key1 = v1.to_string();
        let mut key2 = v2.to_string();
        key1.push_str(column);
        key2.push_str(column);
        mincr_keys.push(key1);
        mincr_keys.push(key2);
    }
    let value1 = format!("repost:{},like:{},comment:{}", v1 + 1, v1 + 1, v1 + 1);
    let value2 = format!("repost:{},like:{},comment:{}", v2 + 1, v2 + 1, v2 + 1);

    let _: () = redis::cmd("MINCR")
        .arg(mincr_keys)
        .query(&mut get_conn())
        .map_err(|e| panic!("set error:{:?}", e))
        .expect("mincr err");

    assert_eq!(
        redis::cmd("MGET").arg(v1).arg(v2).query(&mut get_conn()),
        Ok((value1, value2))
    );
}

//测试场景：decrby只能decby 指定key的指定列 decr 323456789.repost 3
//单独decrby某一列 get到的value为value-1

//流程
// 1. set 每一列
// 2. get 每一列验证是否set成功
// 3. decrby 每一列后再get是否decr 成功decr 323456789.repost
//4. decr整个key decr 323456789 报 Invalid key

#[test]
fn test_basic_decr() {
    let key: u32 = 323456789;
    let value = 456;
    let decr_num = 4;

    let column_cfg = vec![".repost", ".comment", ".like"];
    for column in column_cfg.iter() {
        let mut key_column = key.to_string();
        key_column.push_str(column);

        let _: () = get_conn()
            .set(&key_column, value)
            .map_err(|e| panic!("set error:{:?}", e))
            .expect("set err");
        assert_eq!(
            redis::cmd("GET").arg(&key_column).query(&mut get_conn()),
            Ok(value)
        );

        assert_eq!(
            get_conn().decr::<&str, u32, u32>(&key_column, decr_num),
            Ok(value - decr_num)
        );

        assert_eq!(
            redis::cmd("GET").arg(&key_column).query(&mut get_conn()),
            Ok(value - decr_num)
        );
    }

    assert_panic!(panic!( "{:?}", get_conn().decr::<u32, u32, String>(key, 1)), String, contains "Invalid key");
}

//获取多个key
#[test]
fn test_mget() {
    let mut key_value = HashMap::new();
    key_value.insert(423456789, 444);
    key_value.insert(523456789, 544);
    key_value.insert(623456789, 644);

    for (k, v) in key_value.iter() {
        test_set_key_value(*k, *v);
        let all_value = format!("repost:{},like:{},comment:{}", v, v, v);

        assert_eq!(
            redis::cmd("GET").arg(k).query(&mut get_conn()),
            Ok(all_value)
        );
    }

    assert_eq!(
        redis::cmd("MGET")
            .arg(423456789)
            .arg(523456789)
            .arg(623456789)
            .query(&mut get_conn()),
        Ok((
            "repost:444,like:444,comment:444".to_string(),
            "repost:544,like:544,comment:544".to_string(),
            "repost:644,like:644,comment:644".to_string()
        ))
    );
}

//获取10000个key
// #[test]
// fn test_thousand_mget() {
//     let mut keys = Vec::new();
//     let mut value = Vec::new();

//     for i in 1..=10 {
//         keys.push(i);
//         let all_value = format!("repost:{},like:{},comment:{}", i, i, i);
//         value.push(all_value);
//     }

//     // //let _a: () = get_conn().get(keys).unwrap();
//     // let _mgetr: () = redis::cmd("MGET")
//     //     .arg(keys)
//     //     .query(&mut get_conn())
//     //     .map_err(|e| panic!("mget thou() error:{:?}", e))
//     //     .expect("mget thousand() err");
//     // //let anow = Instant::now();

//     // //println!("{:?}", (anow - now.elapsed()).elapsed());
//     assert_eq!(
//         redis::cmd("MGET").arg(keys).query(&mut get_conn()),
//         Ok(value)
//     );
// }
// todo:如果value大于配置的value 为异常case
// 测试端口配置了三列 repost:value为12b comment:value为10b like:value为10b
// 大于value位数仍然可以存储，有扩展存储
#[test]
fn test_big_value() {
    test_set_key_value(666666, 10000000000000000);
}

fn get_conn() -> Connection {
    let host = file!().get_host();
    let host_clinet = String::from("redis://") + &host;

    let client_rs = Client::open(host_clinet);
    assert!(!client_rs.is_err(), "get client err:{:?}", client_rs.err());

    let client = client_rs
        .map_err(|e| panic!("client error:{:?}", e))
        .expect("client err");
    let conn = client
        .get_connection()
        .map_err(|e| panic!("get_conn() error:{:?}", e))
        .expect("get_conn() err");
    conn
}

fn test_set_key_value(key: i32, value: i64) {
    let column_cfg = vec![".repost", ".comment", ".like"];

    for column in column_cfg.iter() {
        let mut key_column = key.to_string();
        key_column.push_str(column);

        let _: () = get_conn()
            .set(&key_column, value)
            .map_err(|e| panic!("set error:{:?}", e))
            .expect("set err");

        assert_eq!(
            redis::cmd("GET").arg(key_column).query(&mut get_conn()),
            Ok(value)
        );
    }
}
