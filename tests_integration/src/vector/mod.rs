use crate::ci::env::*;
use crate::redis_helper::*;
#[allow(unused)]
use function_name::named;

const RESTYPE: &str = "vector";

#[test]
#[named]
fn vrange_basic() {
    let argkey = function_name!();
    let mut con = get_conn(&RESTYPE.get_host());

    let rsp = redis::cmd("vrange")
        .arg(format!("{argkey},2105"))
        .arg("field")
        .arg("a,b")
        .arg("where")
        .arg("a")
        .arg("=")
        .arg("1")
        .arg("b")
        .arg("in")
        .arg("2,3")
        .arg("order")
        .arg("a")
        .arg("desc")
        .arg("limit")
        .arg("12")
        .arg("24")
        .query(&mut con);
    assert_eq!(rsp, Ok(32));
}

// 返回0条数据
#[test]
fn vrange_0() {
    let mut con = get_conn(&RESTYPE.get_host());
    let rsp: Result<String, redis::RedisError> = redis::cmd("vrange")
        .arg("4668741184209284,2211")
        .arg("field")
        .arg("uid,object_type")
        .arg("where")
        .arg("like_id")
        .arg("=")
        .arg("4968741184209249")
        .arg("limit")
        .arg("0")
        .arg("10")
        .query(&mut con);
    // assert_eq!(rsp, Ok(32));
    println!("++ rsp:{:?}", rsp);
}

#[test]
// 返回1条数据
fn vrange_1() {
    let mut con = get_conn(&RESTYPE.get_host());
    let rsp = redis::cmd("vrange")
        .arg("12345,2211")
        .arg("field")
        .arg("uid,object_type")
        .arg("where")
        .arg("like_id")
        .arg("=")
        .arg("4968741184209211")
        .arg("limit")
        .arg("0")
        .arg("10")
        .query(&mut con);
    assert_eq!(rsp, Ok(32));
}
#[test]
// 返回2条数据
fn vrange_2() {
    let mut con = get_conn(&RESTYPE.get_host());
    let rsp = redis::cmd("vrange")
        .arg("4668741184209282,2211")
        .arg("field")
        .arg("uid,object_type")
        .arg("where")
        .arg("object_id")
        .arg("=")
        .arg("4968741184209227")
        .arg("limit")
        .arg("0")
        .arg("10")
        .query(&mut con);
    assert_eq!(rsp, Ok(32));
}
#[test]
// 返回3条数据
fn vrange_3() {
    let mut con = get_conn(&RESTYPE.get_host());
    let rsp: Result<i32, redis::RedisError> = redis::cmd("vrange")
        .arg("4668741184209283,2211")
        .arg("field")
        .arg("uid,object_type")
        .arg("where")
        .arg("like_id")
        .arg("=")
        .arg("4968741184209237")
        .arg("limit")
        .arg("0")
        .arg("10")
        .query(&mut con);
    assert_eq!(rsp, Ok(32));
}
