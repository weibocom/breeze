use std::{sync::atomic::AtomicI64, thread::sleep, time::Duration};

use redis::{RedisError, Value};

use super::byme::*;

use crate::{
    ci::env::*,
    redis_helper::*,
    vector::{assist::*, RESTYPE},
};

const YEAR_MONTH: &str = "2410";
static LIKE_ID: AtomicI64 = AtomicI64::new(5078096628678703);

// 构建一个新的like id，避免请求重复
fn next_like_id() -> i64 {
    // 每次本地测试，可以调整，ci不用
    let offset = 100;
    let id = LIKE_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    offset + id
}

#[test]
fn aggregation_vrange() -> Result<(), RedisError> {
    let mut conn = get_conn(&RESTYPE.get_host());
    let like_by_me = LikeByMe {
        uid: 7916804453,
        like_id: next_like_id(),
        object_id: 5078096628678703,
        object_type: 1,
    };

    // vrange 获取最新的10条
    let rsp = aglike_cmd_vadd(&mut conn, YEAR_MONTH, &like_by_me)?;
    assert_eq!(rsp, 2);
    let rsp = aglike_cmd_vrange(&mut conn, &like_by_me, 10);
    assert!(rsp.is_ok());

    Ok(())
}

#[test]
fn aggregation_vrange_timeline() -> Result<(), RedisError> {
    let mut conn = get_conn(&RESTYPE.get_host());
    let like_by_me = LikeByMe {
        uid: 79168044531,
        like_id: next_like_id(),
        object_id: 5078096628678703,
        object_type: 1,
    };

    let rsp = aglike_cmd_vadd(&mut conn, YEAR_MONTH, &like_by_me)?;
    assert_eq!(rsp, 2);

    // vrange 获取最新的10条
    let rsp = aglike_cmd_vrange_timeline(&mut conn, YEAR_MONTH, &like_by_me, 10)?;
    println!("++ rsp:{:?}", rsp);
    Ok(())
}

#[test]
fn aggregation_vrange_si() -> Result<(), RedisError> {
    let mut conn = get_conn(&RESTYPE.get_host());
    let like_by_me = LikeByMe {
        uid: 1761220674,
        like_id: next_like_id(),
        object_id: 5078096628678703,
        object_type: 1,
    };

    let rsp = aglike_cmd_vadd(&mut conn, YEAR_MONTH, &like_by_me)?;
    assert_eq!(rsp, 2);

    // 获取最新若干条si记录
    let rsp = aglike_cmd_vrange_si(&mut conn, &like_by_me);
    println!("vrange.si rsp:{:?}", rsp);
    Ok(())
}

#[test]
fn aggregation_vcard() -> Result<(), RedisError> {
    let mut conn = get_conn(&RESTYPE.get_host());
    let like_by_me = LikeByMe {
        uid: 7916804453,
        like_id: next_like_id(),
        object_id: 5078096628678703,
        object_type: 1,
    };

    let rsp = aglike_cmd_vadd(&mut conn, YEAR_MONTH, &like_by_me)?;
    assert_eq!(rsp, 2);

    let by_me_si = aglike_cmd_vcard(&mut conn, like_by_me.uid);
    println!("si: {:?}", by_me_si);
    Ok(())
}

#[test]
fn aggregation_vget() -> Result<(), RedisError> {
    let mut conn = get_conn(&RESTYPE.get_host());
    let like_by_me = LikeByMe {
        uid: 791680445,
        like_id: next_like_id(),
        object_id: 5078096628678703,
        object_type: 1,
    };

    // vadd 加一个id较大的like_by_me，同时更新si、timeline
    let rsp = aglike_cmd_vadd(&mut conn, YEAR_MONTH, &like_by_me)?;
    assert_eq!(rsp, 2);

    sleep(Duration::from_secs(2));

    // vget 获取某个月份最新的1条
    let rsp = aglike_cmd_vget(&mut conn, &like_by_me)?;

    println!("++ vget rsp:{:?}", rsp);

    // 删除新插入的记录
    let rsp: Result<u32, redis::RedisError> = aglike_cmd_vdel(&mut conn, YEAR_MONTH, &like_by_me);
    assert_eq!(Ok(2), rsp);
    Ok(())
}

#[test]
fn aggregation_vadd() -> Result<(), RedisError> {
    let mut conn = get_conn(&RESTYPE.get_host());
    let like_by_me = LikeByMe {
        uid: 791680445,
        like_id: next_like_id(),
        object_id: 5078096628678703,
        object_type: 1,
    };

    // vadd 加一个id较大的like_by_me，同时更新si、timeline
    let rsp = aglike_cmd_vadd(&mut conn, YEAR_MONTH, &like_by_me)?;
    println!("+++  vadd rsp:{:?}", rsp);
    assert!(rsp == (1 + 1) || rsp == (2 + 1));

    // vrange 获取最新的1条
    let like_via_vget = aglike_cmd_vget(&mut conn, &like_by_me);
    println!("++ rsp:{:?}", like_via_vget);

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
        like_via_vget
    );

    // 删除新插入的记录
    let rsp = aglike_cmd_vdel(&mut conn, YEAR_MONTH, &like_by_me);
    assert_eq!(Ok(2), rsp);
    Ok(())
}

#[test]
fn aggregation_vadd_timeline() -> Result<(), RedisError> {
    let mut conn = get_conn(&RESTYPE.get_host());
    let like_by_me = LikeByMe {
        uid: 791680445,
        like_id: next_like_id(),
        object_id: 5078096628678703,
        object_type: 1,
    };

    let rsp = aglike_cmd_vadd_timeline(&mut conn, YEAR_MONTH, &like_by_me);
    println!("vadd timeline:{:?}", rsp);

    Ok(())
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
    let rsp = aglike_cmd_vadd_si(&mut conn, YEAR_MONTH, &si)?;
    println!("+++ add.si rsp:{:?}", rsp);
    assert!(rsp == 1 || rsp == 2);

    let by_me_si = aglike_cmd_vcard(&mut conn, si.uid)?;
    print!("after vadd si, vcard now: {:?}", by_me_si);

    Ok(())
}

#[test]
fn aggregation_vupdate() -> Result<(), RedisError> {
    let mut conn = get_conn(&RESTYPE.get_host());
    let like_by_me = LikeByMe {
        uid: 791680445,
        like_id: next_like_id(),
        object_id: 5078096628678703,
        object_type: 1,
    };

    let rsp = aglike_cmd_vupdate(&mut conn, YEAR_MONTH, &like_by_me);
    assert!(rsp.is_err());
    println!("vupdate rsp should be err: {:?}", rsp);
    Ok(())
}

#[test]
fn aggregation_vupdate_timeline() -> Result<(), RedisError> {
    let mut conn = get_conn(&RESTYPE.get_host());
    let like_id = next_like_id();
    let like_by_me = LikeByMe {
        uid: 791680445,
        like_id: like_id,
        object_id: 5078096628678703,
        object_type: 1,
    };
    let like_by_me2 = LikeByMe {
        uid: 791680445,
        like_id: like_id,
        object_id: 5078096628678703,
        object_type: 2,
    };

    let rsp = aglike_cmd_vadd_timeline(&mut conn, YEAR_MONTH, &like_by_me)?;
    assert!(rsp == 1 || rsp == 2);
    let rsp = aglike_cmd_vupdate(&mut conn, YEAR_MONTH, &like_by_me2)?;
    assert_eq!(rsp, 2);
    println!("vupdate timeline rsp: {}", rsp);
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
    let rsp = aglike_cmd_vadd_si(&mut conn, YEAR_MONTH, &si)?;
    assert!(rsp == 2 || rsp == 1);

    let rsp = aglike_cmd_vupdate_si(&mut conn, YEAR_MONTH, &si)?;

    println!("+++ vupdate.si/{} rsp:{:?}", si.count, rsp);
    assert!(rsp == 1 || rsp == 2);

    let by_me_si = aglike_cmd_vcard(&mut conn, si.uid)?;
    print!("after vupdate si, vcard now: {:?}", by_me_si);

    println!("si: {:?}", by_me_si);

    Ok(())
}

#[test]
fn aggregation_vdel() -> Result<(), RedisError> {
    let mut conn = get_conn(&RESTYPE.get_host());
    let like_by_me = LikeByMe {
        uid: 791680445,
        like_id: next_like_id(),
        object_id: 5078096628678703,
        object_type: 1,
    };

    let vadd_rsp = aglike_cmd_vadd(&mut conn, YEAR_MONTH, &like_by_me)?;
    println!("+++ vadd rsp:{:?}", vadd_rsp);
    assert!(vadd_rsp == (1 + 1) || vadd_rsp == (2 + 1));

    // 删除新插入的记录
    let rsp = aglike_cmd_vdel(&mut conn, YEAR_MONTH, &like_by_me);
    assert_eq!(Ok(2), rsp);
    Ok(())
}

#[test]
fn aggregation_vdel_timeline() -> Result<(), RedisError> {
    let mut conn = get_conn(&RESTYPE.get_host());
    let like_by_me = LikeByMe {
        uid: 791680445,
        like_id: next_like_id(),
        object_id: 5078096628678703,
        object_type: 1,
    };

    let vadd_rsp = aglike_cmd_vadd_timeline(&mut conn, YEAR_MONTH, &like_by_me)?;
    assert_eq!(vadd_rsp, 1);

    let vdel_rsp = aglike_cmd_vdel_timeline(&mut conn, YEAR_MONTH, &like_by_me)?;
    assert_eq!(vdel_rsp, 1);
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

    let rsp = aglike_cmd_vadd_si(&mut conn, YEAR_MONTH, &si)?;
    assert!(rsp == 2 || rsp == 3);

    // vadd 加一个id较大的like_by_me，同时更新si、timeline
    let rsp: Result<u32, redis::RedisError> = aglike_cmd_vdel_si(&mut conn, YEAR_MONTH, &si);
    println!("+++ vdel.si/{} rsp:{:?}", si.count, rsp);
    let rsp = rsp.unwrap();
    assert!(rsp == 1 || rsp == 2);

    let value = aglike_cmd_vcard(&mut conn, si.uid)?;
    print!("after vdel si, vcard now: {:?}", value);
    Ok(())
}
