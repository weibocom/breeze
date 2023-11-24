use crate::ci::env::*;
use crate::redis_helper::*;
#[allow(unused)]
use function_name::named;
use redis::Value;

const RESTYPE: &str = "vector";

#[test]
#[named]
#[ignore]
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
    let rsp = redis::cmd("vrange")
        .arg("4668741184209284,2211")
        .arg("field")
        .arg("uid,object_type")
        .arg("where")
        .arg("like_id")
        .arg("=")
        .arg("4968741184209240")
        .arg("limit")
        .arg("0")
        .arg("10")
        .query(&mut con);
    println!("++ rsp:{:?}", rsp);
    assert_eq!(rsp, Ok(Value::Nil));
}

#[test]
// 返回1条数据
fn vrange_1() {
    let mut con = get_conn(&RESTYPE.get_host());
    let rsp = redis::cmd("vrange")
        .arg("4668741184209281,2211")
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
    println!("++ rsp:{:?}", rsp);
    assert_eq!(
        rsp,
        Ok(Value::Bulk(vec![
            Value::Bulk(vec![
                Value::Status("uid".to_string()),
                Value::Status("object_type".to_string())
            ]),
            Value::Bulk(vec![
                Value::Data("4668741184209281".as_bytes().to_vec()),
                Value::Data("4".as_bytes().to_vec())
            ])
        ]))
    );
}
#[test]
// 返回2条数据
fn vrange_2() {
    let mut con = get_conn(&RESTYPE.get_host());
    let rsp = redis::cmd("vrange")
        .arg("4668741184209282,2211")
        .arg("field")
        .arg("uid,like_id")
        .arg("where")
        .arg("object_id")
        .arg("=")
        .arg("4968741184209227")
        .arg("limit")
        .arg("0")
        .arg("10")
        .query(&mut con);
    println!("++ rsp:{:?}", rsp);
    assert_eq!(
        rsp,
        Ok(Value::Bulk(vec![
            Value::Bulk(vec![
                Value::Status("uid".to_string()),
                Value::Status("like_id".to_string())
            ]),
            Value::Bulk(vec![
                Value::Data("4668741184209282".as_bytes().to_vec()),
                Value::Data("4968741184209225".as_bytes().to_vec()),
                Value::Data("4668741184209282".as_bytes().to_vec()),
                Value::Data("4968741184209226".as_bytes().to_vec())
            ])
        ]))
    );
}
#[test]
// 返回3条数据
fn vrange_3() {
    let mut con = get_conn(&RESTYPE.get_host());
    let rsp = redis::cmd("vrange")
        .arg("4668741184209283,2211")
        .arg("field")
        .arg("uid,object_id")
        .arg("where")
        .arg("like_id")
        .arg("=")
        .arg("4968741184209237")
        .arg("limit")
        .arg("0")
        .arg("10")
        .query(&mut con);
    println!("++ rsp:{:?}", rsp);
    assert_eq!(
        rsp,
        Ok(Value::Bulk(vec![
            Value::Bulk(vec![
                Value::Status("uid".to_string()),
                Value::Status("object_id".to_string())
            ]),
            Value::Bulk(vec![
                Value::Data("4668741184209283".as_bytes().to_vec()),
                Value::Data("4968741184209230".as_bytes().to_vec()),
                Value::Data("4668741184209283".as_bytes().to_vec()),
                Value::Data("4968741184209231".as_bytes().to_vec()),
                Value::Data("4668741184209283".as_bytes().to_vec()),
                Value::Data("4968741184209232".as_bytes().to_vec())
            ])
        ]))
    );
}
