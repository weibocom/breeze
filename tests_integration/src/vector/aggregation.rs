use std::{thread::sleep, time::Duration};

use redis::{RedisError, Value};

use super::byme::*;

use crate::{ci::env::*, redis_helper::*, vector::RESTYPE};

#[test]
fn aggregation_vrange() {
    let mut conn = get_conn(&RESTYPE.get_host());
    let like_by_me = LikeByMe {
        uid: 7916804453,
        like_id: 5078096628678703,
        object_id: 5078096628678703,
        object_type: 1,
    };

    // vrange 获取最新的1条
    let rsp: Result<Value, RedisError> = redis::cmd("VRANGE")
        .arg(format!("{}", like_by_me.uid))
        .arg("field")
        .arg("like_id,object_id,object_type")
        .arg("where")
        .arg("object_type")
        .arg("in")
        .arg("0,1,100")
        .arg("order")
        .arg("desc")
        .arg("like_id")
        .arg("limit")
        .arg("0")
        .arg("10")
        .query(&mut conn);

    println!("++ rsp:{:?}", rsp);
}

#[test]
fn aggregation_vdel_like() {
    let mut conn = get_conn(&RESTYPE.get_host());
    let like_by_me = LikeByMe {
        uid: 791680445,
        like_id: 5078096628678703,
        object_id: 5078096628678703,
        object_type: 1,
    };

    // vadd 加一个id较大的like_by_me，同时更新si、timeline
    // let rsp: Result<u32, redis::RedisError> = redis::cmd("vadd")
    //     .arg(format!("{},2409", like_by_me.uid))
    //     .arg("object_type")
    //     .arg(like_by_me.object_type)
    //     .arg("like_id")
    //     .arg(like_by_me.like_id)
    //     .arg("object_id")
    //     .arg(like_by_me.object_id)
    //     .query(&mut conn);

    // println!("+++ vadd rsp:{:?}", rsp);
    // let rsp = rsp.unwrap();
    // assert!(rsp == (1 + 1) || rsp == (2 + 1));
    // sleep(Duration::from_secs(2));

    // 删除新插入的记录
    let rsp: Result<u32, redis::RedisError> = redis::cmd("vdel")
        .arg(format!("{},2409", like_by_me.uid))
        .arg("where")
        .arg("object_id")
        .arg("=")
        .arg(like_by_me.object_id)
        .arg("object_type")
        .arg("=")
        .arg(like_by_me.object_type)
        .query(&mut conn);

    assert_eq!(Ok(2), rsp);
}

#[test]
fn aggregation_vget() {
    let mut conn = get_conn(&RESTYPE.get_host());
    let like_by_me = LikeByMe {
        uid: 791680445,
        like_id: 5078096628678703,
        object_id: 5078096628678703,
        object_type: 1,
    };

    // vadd 加一个id较大的like_by_me，同时更新si、timeline
    let rsp: Result<u32, redis::RedisError> = redis::cmd("vadd")
        .arg(format!("{},2409", like_by_me.uid))
        .arg("object_type")
        .arg(like_by_me.object_type)
        .arg("like_id")
        .arg(like_by_me.like_id)
        .arg("object_id")
        .arg(like_by_me.object_id)
        .query(&mut conn);

    println!("+++ vadd rsp:{:?}", rsp);
    let rsp = rsp.unwrap();
    assert!(rsp == (1 + 1) || rsp == (2 + 1));
    sleep(Duration::from_secs(2));

    // vrange 获取最新的1条
    let rsp: Result<Value, RedisError> = redis::cmd("vget")
        .arg(format!("{},2409", like_by_me.uid))
        .arg("field")
        .arg("object_type,like_id,object_id")
        .arg("where")
        .arg("like_id")
        .arg("=")
        .arg(like_by_me.like_id)
        .arg("object_type")
        .arg("=")
        .arg(like_by_me.object_type)
        .arg("object_id")
        .arg("=")
        .arg(like_by_me.object_id)
        .query(&mut conn);

    println!("++ rsp:{:?}", rsp);

    // 删除新插入的记录
    let rsp: Result<u32, redis::RedisError> = redis::cmd("vdel")
        .arg(format!("{},2409", like_by_me.uid))
        .arg("where")
        .arg("object_id")
        .arg("=")
        .arg(like_by_me.object_id)
        .arg("object_type")
        .arg("=")
        .arg(like_by_me.object_type)
        .query(&mut conn);

    assert_eq!(Ok(2), rsp);
}

#[test]
fn aggregation_vadd_vrange_vdel() {
    let mut conn = get_conn(&RESTYPE.get_host());
    let like_by_me = LikeByMe {
        uid: 791680445,
        like_id: 5078096628678703,
        object_id: 5078096628678703,
        object_type: 1,
    };

    // vadd 加一个id较大的like_by_me，同时更新si、timeline
    let rsp: Result<u32, redis::RedisError> = redis::cmd("vadd")
        .arg(format!("{},2409", like_by_me.uid))
        .arg("object_type")
        .arg(like_by_me.object_type)
        .arg("like_id")
        .arg(like_by_me.like_id)
        .arg("object_id")
        .arg(like_by_me.object_id)
        .query(&mut conn);

    println!("+++ rsp:{:?}", rsp);
    let rsp = rsp.unwrap();
    assert!(rsp == (1 + 1) || rsp == (2 + 1));

    // vrange 获取最新的1条
    let rsp = redis::cmd("VRANGE")
        .arg(format!("{}", like_by_me.uid))
        .arg("field")
        .arg("uid,object_type,like_id,object_id")
        .arg("where")
        .arg("order")
        .arg("desc")
        .arg("like_id")
        .arg("limit")
        .arg("0")
        .arg("1")
        .query(&mut conn);
    println!("++ rsp:{:?}", rsp);

    assert_eq!(
        Ok(Value::Bulk(vec![
            Value::Bulk(vec![
                Value::Status("uid".to_string().into()),
                Value::Status("object_type".to_string().into()),
                Value::Status("like_id".to_string().into()),
                Value::Status("object_id".to_string().into()),
            ]),
            Value::Bulk(vec![
                Value::Int(like_by_me.uid),
                Value::Int(like_by_me.object_type),
                Value::Int(like_by_me.like_id),
                Value::Int(like_by_me.object_id),
            ]),
        ])),
        rsp
    );

    // 删除新插入的记录
    let rsp: Result<u32, redis::RedisError> = redis::cmd("vdel")
        .arg(format!("{},2409", like_by_me.uid))
        .arg("where")
        .arg("object_id")
        .arg("=")
        .arg(like_by_me.object_id)
        .arg("object_type")
        .arg("=")
        .arg(like_by_me.object_type)
        .query(&mut conn);

    assert_eq!(Ok(2), rsp);
}

#[test]
fn aggregation_vrange_timeline() {
    let mut conn = get_conn(&RESTYPE.get_host());
    let like_by_me = LikeByMe {
        uid: 79168044531,
        like_id: 5078096628678703,
        object_id: 5078096628678703,
        object_type: 1,
    };

    // vrange 获取最新的1条
    let rsp: Result<Value, RedisError> = redis::cmd("VRANGE.timeline")
        .arg(format!("{},2409", like_by_me.uid))
        .arg("field")
        .arg("like_id,object_id,object_type")
        .arg("where")
        .arg("object_type")
        .arg("in")
        .arg("0,1,100")
        .arg("order")
        .arg("desc")
        .arg("like_id")
        .arg("limit")
        .arg("0")
        .arg("10")
        .query(&mut conn);

    println!("++ rsp:{:?}", rsp);
}

#[test]
fn aggregation_vrange_si() {
    let mut conn = get_conn(&RESTYPE.get_host());
    let like_by_me = LikeByMe {
        uid: 1761220674,
        like_id: 5078096628678703,
        object_id: 5078096628678703,
        object_type: 1,
    };
    // 获取最新若干条si记录
    let rsp: Result<Value, RedisError> = redis::cmd("VRANGE.si")
        .arg(format!("{}", like_by_me.uid))
        .arg("field")
        .arg("uid,start_date,sum(count)")
        .arg("where")
        .arg("object_type")
        .arg("in")
        .arg("0,1,100")
        .arg("group")
        .arg("by")
        .arg("uid,start_date")
        .arg("order")
        .arg("desc")
        .arg("start_date")
        .query(&mut conn);
    println!("vrange.si rsp:{:?}", rsp);
}

#[test]
fn aggregation_vadd_si() -> Result<(), RedisError> {
    let mut conn = get_conn(&RESTYPE.get_host());
    let si = LikeByMeSi {
        uid: 7916804459,
        object_type: 3,
        start_date: "2024-09-01".to_string(),
        count: 3,
    };

    // vadd 加一个id较大的like_by_me，同时更新si、timeline
    let rsp: Result<u32, redis::RedisError> = redis::cmd("vadd.si")
        .arg(format!("{},2410", si.uid))
        .arg("object_type")
        .arg(si.object_type)
        // .arg("start_date")
        // .arg(si.start_date)
        .arg("count")
        .arg(si.count)
        .query(&mut conn);

    println!("+++ add.si rsp:{:?}", rsp);
    let rsp = rsp.unwrap();
    assert!(rsp == 1 || rsp == 2);

    let by_me_si: Value = redis::cmd("vcard")
        .arg(format!("{}", si.uid))
        .arg("field")
        .arg("object_type")
        .arg("where")
        .arg("object_type")
        .arg("in")
        .arg("1,2,3,100")
        .arg("group")
        .arg("by")
        .arg("object_type")
        .query(&mut conn)?;

    println!("si: {:?}", by_me_si);

    Ok(())
}

#[test]
fn aggregation_vupdate_si() -> Result<(), RedisError> {
    let mut conn = get_conn(&RESTYPE.get_host());
    let si = LikeByMeSi {
        uid: 7916804459,
        object_type: 3,
        start_date: "2024-09-01".to_string(),
        count: 2,
    };

    // vadd 加一个id较大的like_by_me，同时更新si、timeline
    let rsp: Result<u32, redis::RedisError> = redis::cmd("vupdate.si")
        .arg(format!("{},2410", si.uid))
        // .arg("object_type")
        // .arg(si.object_type)
        // .arg("start_date")
        // .arg(si.start_date)
        .arg("count")
        .arg(si.count)
        .arg("where")
        .arg("object_type")
        .arg("=")
        .arg(si.object_type)
        .query(&mut conn);

    println!("+++ vupdate.si/{} rsp:{:?}", si.count, rsp);
    let rsp = rsp.unwrap();
    assert!(rsp == 1 || rsp == 2);

    let by_me_si: Value = redis::cmd("vcard")
        .arg(format!("{}", si.uid))
        .arg("field")
        .arg("object_type")
        .arg("where")
        .arg("object_type")
        .arg("in")
        .arg("1,2,3,100")
        .arg("group")
        .arg("by")
        .arg("object_type")
        .query(&mut conn)?;

    println!("si: {:?}", by_me_si);

    Ok(())
}

#[test]
fn aggregation_vdel_si() -> Result<(), RedisError> {
    let mut conn = get_conn(&RESTYPE.get_host());
    let si = LikeByMeSi {
        uid: 7916804459,
        object_type: 3,
        start_date: "2024-10-01".to_string(),
        count: 2,
    };

    // vadd 加一个id较大的like_by_me，同时更新si、timeline
    let rsp: Result<u32, redis::RedisError> = redis::cmd("vdel.si")
        .arg(format!("{},2410", si.uid))
        .arg("where")
        .arg("object_type")
        .arg("=")
        .arg(si.object_type)
        .query(&mut conn);

    println!("+++ vupdate.si/{} rsp:{:?}", si.count, rsp);
    let rsp = rsp.unwrap();
    assert!(rsp == 1 || rsp == 2);

    let by_me_si: Value = redis::cmd("vcard")
        .arg(format!("{}", si.uid))
        .arg("field")
        .arg("object_type")
        .arg("where")
        .arg("object_type")
        .arg("in")
        .arg("1,2,3,100")
        .arg("group")
        .arg("by")
        .arg("object_type")
        .query(&mut conn)?;

    println!("si: {:?}", by_me_si);

    Ok(())
}
