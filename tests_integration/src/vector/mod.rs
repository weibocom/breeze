use crate::ci::env::*;
use crate::redis_helper::*;
use byme::LikeByMe;
#[allow(unused)]
use function_name::named;
use redis::Value;
mod aggregation;
mod byme;
mod assist;
mod kvtime;

pub(crate) const RESTYPE: &str = "vector";
pub(crate) const CMD_VGET: &str = "vget";
pub(crate) const CMD_VRANGE: &str = "vrange";

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
#[test]
fn vget_key0() {
    let mut con = get_conn(&RESTYPE.get_host());
    let rsp: Result<_, redis::RedisError> = redis::cmd("vget")
        .arg("0")
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
        .query::<i32>(&mut con);
    println!("++ rsp:{:?}", rsp);
    assert!(rsp.err().is_some());
}

// 返回0条数据
fn vrange_or_vget_0_with_empty_rs(cmd: &str) {
    let mut con = get_conn(&RESTYPE.get_host());
    let like_by_me = LikeByMe {
        uid: 46687411842092840,
        like_id: 4968741184209240,
        object_id: 4968741184209220,
        object_type: 40,
    };
    let uid_unknown = 99999;

    safe_add(&mut con, &like_by_me);

    let rsp = redis::cmd(cmd)
        .arg(format!("{},2211", uid_unknown))
        .arg("field")
        .arg("uid,object_type")
        .arg("where")
        .arg("like_id")
        .arg("=")
        .arg(like_by_me.like_id)
        .arg("limit")
        .arg("0")
        .arg("10")
        .query(&mut con);
    println!("++ rsp:{:?}", rsp);
    assert_eq!(rsp, Ok(Value::Nil));
}

// 返回1条数据
fn vrange_or_vget_1_with_1rows(cmd: &str) {
    let mut con = get_conn(&RESTYPE.get_host());
    let like_by_me = LikeByMe {
        uid: 46687411842092841,
        like_id: 4968741184209241,
        object_id: 4968741184209221,
        object_type: 41,
    };

    safe_add(&mut con, &like_by_me);

    let rsp = redis::cmd(cmd)
        .arg(format!("{},2211", like_by_me.uid))
        .arg("field")
        .arg("uid,object_type")
        .arg("where")
        .arg("like_id")
        .arg("=")
        .arg(like_by_me.like_id)
        .arg("uid")
        .arg("=")
        .arg(like_by_me.uid)
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
                Value::Status("object_type".to_string()),
            ]),
            Value::Bulk(vec![
                Value::Int(like_by_me.uid),
                Value::Int(like_by_me.object_type),
            ])
        ]))
    );
}
// 返回2条数据
fn vrange_or_vget_2_with_2rows(cmd: &str) {
    let mut con = get_conn(&RESTYPE.get_host());
    let like_by_me1 = LikeByMe {
        uid: 46687411842092842,
        like_id: 4968741184209242,
        object_id: 4968741184209222,
        object_type: 42,
    };
    let like_by_me2 = LikeByMe {
        uid: 46687411842092842,
        like_id: 4968741184209242,
        object_id: 49687411842092222,
        object_type: 42,
    };

    safe_add(&mut con, &like_by_me1);
    safe_add(&mut con, &like_by_me2);

    let rsp = redis::cmd(cmd)
        .arg(format!("{},2211", like_by_me1.uid))
        .arg("field")
        .arg("uid,object_type")
        .arg("where")
        .arg("like_id")
        .arg("=")
        .arg(like_by_me1.like_id)
        .arg("uid")
        .arg("=")
        .arg(like_by_me1.uid)
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
                Value::Status("object_type".to_string()),
            ]),
            Value::Bulk(vec![
                Value::Int(like_by_me1.uid),
                Value::Int(like_by_me1.object_type),
                Value::Int(like_by_me2.uid),
                Value::Int(like_by_me2.object_type)
            ])
        ]))
    );
}
// 返回3条数据
fn vrange_or_vget_3_with_3rows(cmd: &str) {
    let mut con = get_conn(&RESTYPE.get_host());
    let like_by_me1 = LikeByMe {
        uid: 46687411842092843,
        like_id: 4968741184209243,
        object_id: 4968741184209223,
        object_type: 43,
    };
    let like_by_me2 = LikeByMe {
        uid: 46687411842092843,
        like_id: 4968741184209243,
        object_id: 49687411842092232,
        object_type: 43,
    };
    let like_by_me3 = LikeByMe {
        uid: 46687411842092843,
        like_id: 4968741184209243,
        object_id: 49687411842092233,
        object_type: 43,
    };

    safe_add(&mut con, &like_by_me1);
    safe_add(&mut con, &like_by_me2);
    safe_add(&mut con, &like_by_me3);

    let rsp = redis::cmd(cmd)
        .arg(format!("{},2211", like_by_me1.uid))
        .arg("field")
        .arg("uid,object_type")
        .arg("where")
        .arg("like_id")
        .arg("=")
        .arg(like_by_me1.like_id)
        .arg("uid")
        .arg("=")
        .arg(like_by_me1.uid)
        .arg("order")
        .arg("desc")
        .arg("object_type")
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
                Value::Int(like_by_me1.uid),
                Value::Int(like_by_me1.object_type),
                Value::Int(like_by_me2.uid),
                Value::Int(like_by_me2.object_type),
                Value::Int(like_by_me3.uid),
                Value::Int(like_by_me3.object_type)
            ])
        ]))
    );
}

// 返回0条数据
fn vrange_or_vget_4_with_sql_injectrion(cmd: &str) {
    let mut con = get_conn(&RESTYPE.get_host());
    let rsp: Result<Value, redis::RedisError> = redis::cmd(cmd)
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

// 返回0条数据
fn vrange_or_vget_5_without_where(cmd: &str) {
    let mut con = get_conn(&RESTYPE.get_host());
    let rsp: Result<Value, redis::RedisError> = redis::cmd(cmd)
        .arg("4668741184209284,2211")
        .arg("field")
        .arg("uid,object_type")
        .query(&mut con);
    println!("++ rsp:{:?}", rsp);
    assert!(rsp.is_ok());
}

// 返回3条数据
fn vrange_or_vget_6_with_group(cmd: &str) {
    let mut con = get_conn(&RESTYPE.get_host());
    let like_by_me1 = LikeByMe {
        uid: 46687411842092846,
        like_id: 496874118420926,
        object_id: 4968741184209226,
        object_type: 46,
    };
    let like_by_me2 = LikeByMe {
        uid: 46687411842092846,
        like_id: 496874118420926,
        object_id: 49687411842092262,
        object_type: 46,
    };
    let like_by_me3 = LikeByMe {
        uid: 46687411842092846,
        like_id: 496874118420926,
        object_id: 49687411842092263,
        object_type: 46,
    };

    safe_add(&mut con, &like_by_me1);
    safe_add(&mut con, &like_by_me2);
    safe_add(&mut con, &like_by_me3);

    let rsp = redis::cmd(cmd)
        .arg(format!("{},2211", like_by_me1.uid))
        .arg("field")
        .arg("uid,object_id")
        .arg("where")
        .arg("like_id")
        .arg("=")
        .arg(like_by_me1.like_id)
        .arg("group")
        .arg("by")
        .arg("uid,object_id")
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
                Value::Int(like_by_me1.uid),
                Value::Int(like_by_me1.object_id),
                Value::Int(like_by_me2.uid),
                Value::Int(like_by_me2.object_id),
                Value::Int(like_by_me3.uid),
                Value::Int(like_by_me3.object_id)
            ])
        ]))
    );
}

// 返回3条数据
fn vrange_or_vget_7_with_count(cmd: &str) {
    let mut con = get_conn(&RESTYPE.get_host());
    let like_by_me1 = LikeByMe {
        uid: 46687411842092847,
        like_id: 496874118420927,
        object_id: 4968741184209227,
        object_type: 47,
    };
    let like_by_me2 = LikeByMe {
        uid: 46687411842092847,
        like_id: 496874118420927,
        object_id: 49687411842092272,
        object_type: 47,
    };
    let like_by_me3 = LikeByMe {
        uid: 46687411842092847,
        like_id: 496874118420927,
        object_id: 49687411842092273,
        object_type: 47,
    };

    safe_add(&mut con, &like_by_me1);
    safe_add(&mut con, &like_by_me2);
    safe_add(&mut con, &like_by_me3);

    let rsp = redis::cmd(cmd)
        .arg(format!("{},2211", like_by_me1.uid))
        .arg("field")
        .arg("uid,like_id,count(*)")
        .arg("where")
        .arg("like_id")
        .arg("=")
        .arg(like_by_me1.like_id)
        .arg("group")
        .arg("by")
        .arg("uid,like_id")
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
                Value::Status("like_id".to_string()),
                Value::Status("count(*)".to_string())
            ]),
            Value::Bulk(vec![
                Value::Int(like_by_me1.uid),
                Value::Int(like_by_me1.like_id),
                Value::Int(3),
            ])
        ]))
    );
}

// 返回0条数据
#[test]
fn vrange_0_with_empty_rs() {
    vrange_or_vget_0_with_empty_rs(CMD_VRANGE);
}

#[test]
// 返回1条数据
fn vrange_1_with_1rows() {
    vrange_or_vget_1_with_1rows(CMD_VRANGE);
}
#[test]
// 返回2条数据
fn vrange_2_with_2rows() {
    vrange_or_vget_2_with_2rows(CMD_VRANGE);
}
#[test]
// 返回3条数据
fn vrange_3_with_3rows() {
    vrange_or_vget_3_with_3rows(CMD_VRANGE);
}

// 返回0条数据
#[test]
fn vrange_4_with_sql_injectrion() {
    vrange_or_vget_4_with_sql_injectrion(CMD_VRANGE);
}

// 返回0条数据
#[test]
fn vrange_5_without_where() {
    vrange_or_vget_5_without_where(CMD_VRANGE);
}

#[test]
// 返回3条数据
fn vrange_6_with_group() {
    vrange_or_vget_6_with_group(CMD_VRANGE);
}

#[test]
// 返回3条数据
fn vrange_7_with_count() {
    vrange_or_vget_7_with_count(CMD_VRANGE);
}

// 返回0条数据
#[test]
fn vget_0_with_empty_rs() {
    vrange_or_vget_0_with_empty_rs(CMD_VGET);
}

#[test]
// 返回1条数据
fn vget_1_with_1rows() {
    vrange_or_vget_1_with_1rows(CMD_VGET);
}
#[test]
// 返回2条数据
fn vget_2_with_2rows() {
    vrange_or_vget_2_with_2rows(CMD_VGET);
}
#[test]
// 返回3条数据
fn vget_3_with_3rows() {
    vrange_or_vget_3_with_3rows(CMD_VGET);
}

// 返回0条数据
#[test]
fn vget_4_with_sql_injectrion() {
    vrange_or_vget_4_with_sql_injectrion(CMD_VGET);
}

// 返回0条数据
#[test]
fn vget_5_without_where() {
    vrange_or_vget_5_without_where(CMD_VGET);
}

#[test]
// 返回3条数据
fn vget_6_with_group() {
    vrange_or_vget_6_with_group(CMD_VGET);
}

#[test]
// 返回3条数据
fn vget_7_with_count() {
    vrange_or_vget_7_with_count(CMD_VGET);
}

#[test]
fn vcard() {
    let mut con = get_conn(&RESTYPE.get_host());
    let like_by_me1 = LikeByMe {
        uid: 46687411842092848,
        like_id: 496874118420928,
        object_id: 4968741184209228,
        object_type: 48,
    };
    let like_by_me2 = LikeByMe {
        uid: 46687411842092848,
        like_id: 496874118420928,
        object_id: 49687411842092282,
        object_type: 48,
    };
    let like_by_me3 = LikeByMe {
        uid: 46687411842092848,
        like_id: 496874118420928,
        object_id: 49687411842092283,
        object_type: 48,
    };

    safe_add(&mut con, &like_by_me1);
    safe_add(&mut con, &like_by_me2);
    safe_add(&mut con, &like_by_me3);

    let rsp = redis::cmd("vcard")
        .arg(format!("{},2211", like_by_me1.uid))
        .arg("where")
        .arg("like_id")
        .arg("=")
        .arg(like_by_me1.like_id)
        .query(&mut con);
    println!("++ rsp:{:?}", rsp);
    assert_eq!(rsp, Ok(3));
}

#[test]
fn vadd() {
    let mut con = get_conn(&RESTYPE.get_host());
    let like_by_me = LikeByMe {
        uid: 4668741184209288,
        like_id: 4968741184209225,
        object_id: 4968741184209227,
        object_type: 4,
    };

    safe_add(&mut con, &like_by_me);
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

fn safe_add(con: &mut redis::Connection, like_by_me: &LikeByMe) {
    let rsp: Result<i32, redis::RedisError> = redis::cmd("vdel")
        .arg(format!("{},2211", like_by_me.uid))
        .arg("where")
        .arg("like_id")
        .arg("=")
        .arg(like_by_me.like_id)
        .arg("object_id")
        .arg("=")
        .arg(like_by_me.object_id)
        .arg("object_type")
        .arg("=")
        .arg(like_by_me.object_type)
        .query(con);
    println!("+++ rsp:{:?}", rsp);
    // assert_eq!(rsp, Ok(1));

    let rsp = redis::cmd("vadd")
        .arg(format!("{},2211", like_by_me.uid))
        .arg("object_type")
        .arg(like_by_me.object_type)
        .arg("like_id")
        .arg(like_by_me.like_id)
        .arg("object_id")
        .arg(like_by_me.object_id)
        .query(con);
    println!("+++ rsp:{:?}", rsp);
    assert_eq!(rsp, Ok(1));
}
