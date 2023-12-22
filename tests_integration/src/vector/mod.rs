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
            Value::Bulk(vec![Value::Int(4668741184209281), Value::Int(4)])
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
                Value::Int(4668741184209282),
                Value::Int(4968741184209225),
                Value::Int(4668741184209282),
                Value::Int(4968741184209226)
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
                Value::Status("object_id".to_string()),
            ]),
            Value::Bulk(vec![
                Value::Int(4668741184209283),
                Value::Int(4968741184209230),
                Value::Int(4668741184209283),
                Value::Int(4968741184209231),
                Value::Int(4668741184209283),
                Value::Int(4968741184209232),
            ])
        ]))
    );
}

// 返回0条数据
#[test]
fn vrange_with_sql_injectrion() {
    let mut con = get_conn(&RESTYPE.get_host());
    let rsp: Result<Value, redis::RedisError> = redis::cmd("vrange")
        .arg("4668741184209284,2211")
        .arg("field")
        .arg("uid,object_type,(select 1)")
        .arg("where")
        .arg("like_id")
        .arg("=")
        .arg("4968741184209240")
        .arg("limit")
        .arg("0")
        .arg("10")
        .query(&mut con);
    println!("++ rsp:{:?}", rsp);
    assert!(rsp.is_err());
}

#[test]
// 返回3条数据
fn vrange_with_group() {
    let mut con = get_conn(&RESTYPE.get_host());
    let rsp = redis::cmd("vrange")
        .arg("4668741184209283,2211")
        .arg("field")
        .arg("uid,object_id")
        .arg("where")
        .arg("like_id")
        .arg("=")
        .arg("4968741184209237")
        .arg("group")
        .arg("by")
        .arg("object_id")
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
                Value::Status("object_id".to_string()),
            ]),
            Value::Bulk(vec![
                Value::Int(4668741184209283),
                Value::Int(4968741184209230),
                Value::Int(4668741184209283),
                Value::Int(4968741184209231),
                Value::Int(4668741184209283),
                Value::Int(4968741184209232),
            ])
        ]))
    );
}

#[test]
// 返回3条数据
fn vrange_with_count() {
    let mut con = get_conn(&RESTYPE.get_host());
    let rsp = redis::cmd("vrange")
        .arg("4668741184209283,2211")
        .arg("field")
        .arg("uid,object_id,count(*)")
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
                Value::Status("object_id".to_string()),
                Value::Status("count(*)".to_string())
            ]),
            Value::Bulk(vec![
                Value::Int(4668741184209283),
                Value::Int(4968741184209230),
                Value::Int(3),
            ])
        ]))
    );
}

#[test]
fn vcard() {
    let mut con = get_conn(&RESTYPE.get_host());
    let rsp = redis::cmd("vcard")
        .arg("4668741184209283,2211")
        .arg("where")
        .arg("like_id")
        .arg("=")
        .arg("4968741184209237")
        .query(&mut con);
    println!("++ rsp:{:?}", rsp);
    assert_eq!(rsp, Ok(3));
}

#[test]
fn vadd() {
    let mut con = get_conn(&RESTYPE.get_host());
    let uid = "4668741184209288";
    let like_id = "4968741184209225";
    let object_id = "4968741184209227";
    let object_type = "4";

    let rsp: Result<i32, redis::RedisError> = redis::cmd("vdel")
        .arg(format!("{},2211", uid))
        .arg("where")
        .arg("like_id")
        .arg("=")
        .arg(like_id)
        .arg("object_id")
        .arg("=")
        .arg(object_id)
        .arg("object_type")
        .arg("=")
        .arg(object_type)
        .query(&mut con);
    println!("+++ rsp:{:?}", rsp);
    // assert_eq!(rsp, Ok(1));

    let rsp = redis::cmd("vadd")
        .arg(format!("{},2211", uid))
        .arg("object_type")
        .arg(object_type)
        .arg("like_id")
        .arg(like_id)
        .arg("object_id")
        .arg(object_id)
        .query(&mut con);
    println!("+++ rsp:{:?}", rsp);
    assert_eq!(rsp, Ok(1));
}

#[test]
fn vupdate() {
    let uid = "4668741184209289";
    let like_id = "4968741184209225";
    let object_id = "4968741184209227";
    let object_type = "4";
    let object_type_new = "6";

    let mut con = get_conn(&RESTYPE.get_host());
    let rsp: Result<i32, redis::RedisError> = redis::cmd("vdel")
        .arg(format!("{},2211", uid))
        .arg("where")
        .arg("like_id")
        .arg("=")
        .arg(like_id)
        .arg("object_id")
        .arg("=")
        .arg(object_id)
        .arg("object_type")
        .arg("=")
        .arg(object_type)
        .query(&mut con);
    println!("+++ rsp:{:?}", rsp);

    let rsp = redis::cmd("vadd")
        .arg(format!("{},2211", uid))
        .arg("object_type")
        .arg(object_type)
        .arg("like_id")
        .arg(like_id)
        .arg("object_id")
        .arg(object_id)
        .query(&mut con);
    println!("+++ rsp:{:?}", rsp);
    assert_eq!(rsp, Ok(1));

    let mut con = get_conn(&RESTYPE.get_host());
    let rsp = redis::cmd("vupdate")
        .arg(format!("{},2211", uid))
        .arg("object_type")
        .arg(object_type_new)
        .arg("where")
        .arg("like_id")
        .arg("=")
        .arg(like_id)
        .arg("object_id")
        .arg("=")
        .arg(object_id)
        .arg("object_type")
        .arg("=")
        .arg(object_type)
        .query(&mut con);
    println!("+++ rsp:{:?}", rsp);
    assert_eq!(rsp, Ok(1));

    let rsp = redis::cmd("vupdate")
        .arg(format!("{},2211", uid))
        .arg("object_type")
        .arg(object_type)
        .arg("where")
        .arg("like_id")
        .arg("=")
        .arg(like_id)
        .arg("object_id")
        .arg("=")
        .arg(object_id)
        .arg("object_type")
        .arg("=")
        .arg(object_type_new)
        .query(&mut con);
    println!("+++ rsp:{:?}", rsp);
    assert_eq!(rsp, Ok(1));
}

#[test]
fn vdel() {
    let uid = "4668741184209292";
    let like_id = "4968741184209225";
    let object_id = "4968741184209227";
    let object_type = "4";

    let mut con = get_conn(&RESTYPE.get_host());
    let rsp = redis::cmd("vadd")
        .arg(format!("{},2211", uid))
        .arg("object_type")
        .arg(object_type)
        .arg("like_id")
        .arg(like_id)
        .arg("object_id")
        .arg(object_id)
        .query(&mut con);
    println!("+++ rsp:{:?}", rsp);
    assert_eq!(rsp, Ok(1));

    let rsp = redis::cmd("vdel")
        .arg(format!("{},2211", uid))
        .arg("where")
        .arg("like_id")
        .arg("=")
        .arg(like_id)
        .arg("object_id")
        .arg("=")
        .arg(object_id)
        .arg("object_type")
        .arg("=")
        .arg(object_type)
        .query(&mut con);
    println!("+++ rsp:{:?}", rsp);
    assert_eq!(rsp, Ok(1));

    vadd();
}
