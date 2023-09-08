use crate::ci::env::*;
use crate::redis::RESTYPE;
use crate::redis_helper::*;
use assert_panic::assert_panic;
use function_name::named;
//use std::fmt::Write;
/// val为空字符串
/// key为空字符串
/// hset key为空字符串是error
// #[named]
// #[test]
// fn limit_val_empty() {
//     let argkey = function_name!();
//     let mut con = get_conn(&RESTYPE.get_host());

//     redis::cmd("DEL").arg(argkey).execute(&mut con);
//     redis::cmd("DEL").arg("").execute(&mut con);

//     let empty_string = String::new();

//     redis::cmd("SET")
//         .arg(argkey)
//         .arg(empty_string)
//         .execute(&mut con);
//     assert_eq!(con.get(argkey), Ok("".to_string()));

//     redis::cmd("SET").arg("").arg(1).execute(&mut con);
//     assert_eq!(
//         con.get::<&str, Option<i32>>("")
//             .map_err(|e| panic!("get empty key error:{:?}", e))
//             .expect("get empty key err"),
//         Some(1)
//     );
// }

const ERROR_CONTENT: &str = "invalid bulk num";

/// incr 0 =>wrong number of arguments for 'incr'
/// incr -1=>wrong number of arguments for 'incr'
#[named]
#[test]
fn limit_incr_non_positive() {
    let argkey = function_name!();
    let mut con = get_conn(&RESTYPE.get_host());

    redis::cmd("DEL").arg(argkey).execute(&mut con);
    let _: () = redis::cmd("SET")
        .arg(argkey)
        .arg(44)
        .query(&mut con)
        .map_err(|e| panic!("set error:{:?}", e))
        .expect("set err");

    assert_eq!(
        redis::cmd("GET")
            .arg::<&str>(argkey)
            .query::<Option<i64>>(&mut con)
            .map_err(|e| panic!("set error:{:?}", e))
            .expect("set err"),
        Some(44)
    );

    assert_panic!(panic!( "{:?}", redis::cmd("INCR").arg::<&str>(argkey).arg::<i32>(0).query::<String>(&mut get_conn(&RESTYPE.get_host()))), String, contains ERROR_CONTENT);
    assert_panic!(panic!( "{:?}", redis::cmd("INCR").arg::<&str>(argkey).arg::<i32>(-1).query::<String>(&mut get_conn(&RESTYPE.get_host()))), String, contains ERROR_CONTENT);
}

/// 设置value大小为512
#[named]
#[test]
fn limit_big_value_size() {
    let argkey = function_name!();
    let mut con = get_conn(&RESTYPE.get_host());
    redis::cmd("DEL").arg(argkey).execute(&mut con);
    let maxbyte = [0u8; 512];
    let mut max = String::new();
    for a in maxbyte.iter() {
        max.push_str(&a.to_string());
        //write!(max, "{:02x}", a);
    }
    let _: () = redis::cmd("SET")
        .arg(argkey)
        .arg(max)
        .query(&mut con)
        .map_err(|e| panic!("set error:{:?}", e))
        .expect("set err");
    assert_eq!(
        redis::cmd("GET")
            .arg::<&str>(argkey)
            .query::<Option<i64>>(&mut con)
            .map_err(|e| panic!("get error:{:?}", e))
            .expect("get err"),
        Some(0)
    );
}

///测试命令中参数异常的情况
/// set key 不加val 提示wrong num
/// get 后不加任何值 提示结尾异常
/// mset key 不加val 程序直接异常退出
#[named]
#[test]
fn limit_par_num() {
    let argkey = function_name!();
    let mut con = get_conn(&RESTYPE.get_host());
    redis::cmd("DEL").arg(argkey).execute(&mut con);

    assert_panic!(panic!( "{:?}", redis::cmd("SET").arg::<&str>(argkey).query::<String>(&mut get_conn(&RESTYPE.get_host()))), String, contains ERROR_CONTENT);
    assert_panic!(panic!( "{:?}", redis::cmd("GET").query::<String>(&mut get_conn(&RESTYPE.get_host()))), String, contains ERROR_CONTENT);
    //会直接crash退出程序
    // let _: () = redis::cmd("MSET")
    //     .arg(argkey)
    //     // .arg(max)
    //     .query(&mut con)
    //     .map_err(|e| panic!("mset error:{:?}", e))
    //     .expect("mset err");
}
///list相关的test
/// hsset空key
#[test]
fn limit_key_item() {
    let mut con = get_conn(&RESTYPE.get_host());
    let empty_string: &str = &String::new();
    redis::cmd("DEL").arg(0).execute(&mut con);

    redis::cmd("HSET")
        .arg(empty_string)
        .arg("hash1")
        .arg(1)
        .execute(&mut con);

    // redis::cmd("HSET")
    //     .arg(0)
    //     .arg("hash2")
    //     .arg(1)
    //     .execute(&mut con);
    // assert_panic!(panic!( "{:?}", redis::cmd("HSET").arg::<&str>(&empty_string).arg::<&str>("hash1").arg::<i32>(1).query::<String>(&mut get_conn(&RESTYPE.get_host()))), String, contains "Operation against a key holding the wrong kind of value");
    // assert_eq!(
    //     con.hset_multiple(
    //         argkey,
    //         &[("filed1", 1), ("filed2", 2), ("filed3", 3), ("filed4", 4),]
    //     ),
    //     Ok(true)
    // );
}
