use crate::ci::env::*;
use crate::redis::RESTYPE;
use crate::redis_helper::*;
use assert_panic::assert_panic;
use function_name::named;
use redis::Commands;
//use std::fmt::Write;
/// val为空字符串
/// key为空字符串
/// hset key为空字符串是error
// #[named]
// #[test]
// fn limit_val_empty() {
//     let arykey = function_name!();
//     let mut con = get_conn(&RESTYPE.get_host());

//     redis::cmd("DEL").arg(arykey).execute(&mut con);
//     redis::cmd("DEL").arg("").execute(&mut con);

//     let empty_string = String::new();

//     redis::cmd("SET")
//         .arg(arykey)
//         .arg(empty_string)
//         .execute(&mut con);
//     assert_eq!(con.get(arykey), Ok("".to_string()));

//     redis::cmd("SET").arg("").arg(1).execute(&mut con);
//     assert_eq!(
//         con.get::<&str, Option<i32>>("")
//             .map_err(|e| panic!("get empty key error:{:?}", e))
//             .expect("get empty key err"),
//         Some(1)
//     );
// }

/// incr 0 =>wrong number of arguments for 'incr'
/// incr -1=>wrong number of arguments for 'incr'
#[named]
#[test]
fn limit_incr_non_positive() {
    let arykey = function_name!();
    let mut con = get_conn(&RESTYPE.get_host());

    redis::cmd("DEL").arg(arykey).execute(&mut con);
    let _: () = redis::cmd("SET")
        .arg(arykey)
        .arg(44)
        .query(&mut con)
        .map_err(|e| panic!("set error:{:?}", e))
        .expect("set err");

    assert_eq!(
        redis::cmd("GET")
            .arg::<&str>(arykey)
            .query::<Option<i64>>(&mut con)
            .map_err(|e| panic!("set error:{:?}", e))
            .expect("set err"),
        Some(44)
    );

    assert_panic!(panic!( "{:?}", redis::cmd("INCR").arg::<&str>(arykey).arg::<i32>(0).query::<String>(&mut get_conn(&RESTYPE.get_host()))), String, contains "wrong number");
    assert_panic!(panic!( "{:?}", redis::cmd("INCR").arg::<&str>(arykey).arg::<i32>(-1).query::<String>(&mut get_conn(&RESTYPE.get_host()))), String, contains "wrong number");
}

/// 设置value大小为512
#[named]
#[test]
fn limit_big_value_size() {
    let arykey = function_name!();
    let mut con = get_conn(&RESTYPE.get_host());
    redis::cmd("DEL").arg(arykey).execute(&mut con);
    let maxbyte = [0u8; 512];
    let mut max = String::new();
    for a in maxbyte.iter() {
        max.push_str(&a.to_string());
        //write!(max, "{:02x}", a);
    }
    let _: () = redis::cmd("SET")
        .arg(arykey)
        .arg(max)
        .query(&mut con)
        .map_err(|e| panic!("set error:{:?}", e))
        .expect("set err");
    assert_eq!(
        redis::cmd("GET")
            .arg::<&str>(arykey)
            .query::<Option<i64>>(&mut con)
            .map_err(|e| panic!("get error:{:?}", e))
            .expect("get err"),
        Some(0)
    );
}

#[named]
#[test]
fn limit_par_num() {
    let arykey = function_name!();
    let mut con = get_conn(&RESTYPE.get_host());
    redis::cmd("DEL").arg(arykey).execute(&mut con);

    assert_panic!(panic!( "{:?}", redis::cmd("SET").arg::<&str>(arykey).query::<String>(&mut get_conn(&RESTYPE.get_host()))), String, contains "wrong number");
    assert_panic!(panic!( "{:?}", redis::cmd("GET").query::<String>(&mut get_conn(&RESTYPE.get_host()))), String, contains "unexpected end of file");
    //会直接crash退出程序
    // let _: () = redis::cmd("MSET")
    //     .arg(arykey)
    //     // .arg(max)
    //     .query(&mut con)
    //     .map_err(|e| panic!("mset error:{:?}", e))
    //     .expect("mset err");
}
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
    //         arykey,
    //         &[("filed1", 1), ("filed2", 2), ("filed3", 3), ("filed4", 4),]
    //     ),
    //     Ok(true)
    // );
}
