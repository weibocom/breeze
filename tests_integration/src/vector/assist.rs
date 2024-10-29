/**
 * 存放访问mesh的指令方法，方便服复用
 */
use redis::{Connection, RedisError, Value};

use super::byme::{LikeByMe, LikeByMeSi};

pub(super) fn aglike_cmd_vrange(
    conn: &mut Connection,
    like_by_me: &LikeByMe,
    count: i64,
) -> Result<Value, RedisError> {
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
        .arg(count)
        .query(conn);
    println!("cmd vrange rsp: {:?}", rsp);
    rsp
}

pub(super) fn aglike_cmd_vrange_timeline(
    conn: &mut Connection,
    year_month: &str,
    like_by_me: &LikeByMe,
    count: i64,
) -> Result<Value, RedisError> {
    let rsp: Result<Value, RedisError> = redis::cmd("VRANGE.timeline")
        .arg(format!("{},{}", like_by_me.uid, year_month))
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
        .arg(count)
        .query(conn);
    println!("cmd vrange.timeline rsp: {:?}", rsp);
    rsp
}

pub(super) fn aglike_cmd_vrange_si(
    conn: &mut Connection,
    like_by_me: &LikeByMe,
) -> Result<Value, RedisError> {
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
        .query(conn);
    println!("cmd vrange.si rsp: {:?}", rsp);
    rsp
}

pub(super) fn aglike_cmd_vcard(conn: &mut Connection, uid: i64) -> Result<Value, RedisError> {
    // let mut conn = get_conn(&RESTYPE.get_host());
    let rsp: Result<Value, RedisError> = redis::cmd("vcard")
        .arg(format!("{}", uid))
        .arg("field")
        .arg("object_type")
        .arg("where")
        .arg("object_type")
        .arg("in")
        .arg("1,2,3,100")
        .arg("group")
        .arg("by")
        .arg("object_type")
        .query(conn);

    println!("cmd vcard rsp: {:?}", rsp);
    rsp
}

pub(super) fn aglike_cmd_vadd(
    conn: &mut Connection,
    year_month: &str,
    like_by_me: &LikeByMe,
) -> Result<u32, redis::RedisError> {
    // vadd 加一个id较大的like_by_me，同时更新si、timeline
    let rsp: Result<u32, redis::RedisError> = redis::cmd("vadd")
        .arg(format!("{},{}", like_by_me.uid, year_month))
        .arg("object_type")
        .arg(like_by_me.object_type)
        .arg("like_id")
        .arg(like_by_me.like_id)
        .arg("object_id")
        .arg(like_by_me.object_id)
        .query(conn);
    println!("cmd vadd rsp: {:?}", rsp);
    rsp
}

pub(super) fn aglike_cmd_vadd_timeline(
    conn: &mut Connection,
    year_month: &str,
    like_by_me: &LikeByMe,
) -> Result<u32, redis::RedisError> {
    // vadd 加一个id较大的like_by_me，同时更新si、timeline
    let rsp: Result<u32, redis::RedisError> = redis::cmd("vadd.timeline")
        .arg(format!("{},{}", like_by_me.uid, year_month))
        .arg("uid")
        .arg(like_by_me.uid)
        .arg("object_type")
        .arg(like_by_me.object_type)
        .arg("like_id")
        .arg(like_by_me.like_id)
        .arg("object_id")
        .arg(like_by_me.object_id)
        .query(conn);
    println!("cmd vadd.timeline rsp: {:?}", rsp);
    rsp
}

pub(super) fn aglike_cmd_vadd_si(
    conn: &mut Connection,
    year_month: &str,
    si: &LikeByMeSi,
) -> Result<u32, RedisError> {
    let rsp: Result<u32, RedisError> = redis::cmd("vadd.si")
        .arg(format!("{},{}", si.uid, year_month))
        .arg("object_type")
        .arg(si.object_type)
        .arg("count")
        .arg(si.count)
        .query(conn);
    println!("cmd vadd.si rsp: {:?}", rsp);
    rsp
}

// 在aggregation策略下，这个方法目前是返回error的
pub(super) fn aglike_cmd_vupdate(
    conn: &mut Connection,
    year_month: &str,
    like_by_me: &LikeByMe,
) -> Result<u32, redis::RedisError> {
    let rsp: Result<u32, redis::RedisError> = redis::cmd("vupdate")
        .arg(format!("{},{}", like_by_me.uid, year_month))
        .arg("object_id")
        .arg(like_by_me.object_id)
        .arg("where")
        .arg("object_type")
        .arg("=")
        .arg(like_by_me.object_type)
        .arg("like_id")
        .arg("=")
        .arg(like_by_me.like_id)
        .arg("object_id")
        .arg("=")
        .arg(like_by_me.object_id)
        .query(conn);
    println!("cmd vupdate rsp: {:?}", rsp);
    rsp
}

pub(super) fn aglike_cmd_vupdate_timeline(
    conn: &mut Connection,
    year_month: &str,
    like_by_me: &LikeByMe,
) -> Result<u32, redis::RedisError> {
    let rsp: Result<u32, redis::RedisError> = redis::cmd("vupdate.timeline")
        .arg(format!("{},{}", like_by_me.uid, year_month))
        .arg("object_id")
        .arg(like_by_me.object_id)
        .arg("where")
        .arg("object_type")
        .arg("=")
        .arg(like_by_me.object_type)
        .arg("like_id")
        .arg("=")
        .arg(like_by_me.like_id)
        .arg("object_id")
        .arg("=")
        .arg(like_by_me.object_id)
        .query(conn);
    println!("cmd vupdate.timeline rsp: {:?}", rsp);
    rsp
}

pub(super) fn aglike_cmd_vupdate_si(
    conn: &mut Connection,
    year_month: &str,
    si: &LikeByMeSi,
) -> Result<u32, redis::RedisError> {
    let rsp: Result<u32, redis::RedisError> = redis::cmd("vupdate.si")
        .arg(format!("{},{}", si.uid, year_month))
        .arg("count")
        .arg(si.count)
        .arg("where")
        .arg("object_type")
        .arg("=")
        .arg(si.object_type)
        .query(conn);
    println!("cmd vupdate.si rsp: {:?}", rsp);
    rsp
}

pub(super) fn aglike_cmd_vdel(
    conn: &mut Connection,
    year_month: &str,
    like_by_me: &LikeByMe,
) -> Result<u32, RedisError> {
    let rsp: Result<u32, RedisError> = redis::cmd("vdel")
        .arg(format!("{},{}", like_by_me.uid, year_month))
        .arg("where")
        .arg("object_id")
        .arg("=")
        .arg(like_by_me.object_id)
        .arg("object_type")
        .arg("=")
        .arg(like_by_me.object_type)
        .query(conn);
    println!("cmd vdel rsp: {:?}", rsp);
    rsp
}

pub(super) fn aglike_cmd_vdel_timeline(
    conn: &mut Connection,
    year_month: &str,
    like_by_me: &LikeByMe,
) -> Result<u32, RedisError> {
    let rsp: Result<u32, RedisError> = redis::cmd("vdel.timeline")
        .arg(format!("{},{}", like_by_me.uid, year_month))
        .arg("where")
        .arg("object_id")
        .arg("=")
        .arg(like_by_me.object_id)
        .arg("object_type")
        .arg("=")
        .arg(like_by_me.object_type)
        .arg("like_id")
        .arg("=")
        .arg(like_by_me.like_id)
        .query(conn);
    println!("cmd vdel.timeline rsp: {:?}", rsp);
    rsp
}

pub(super) fn aglike_cmd_vdel_si(
    conn: &mut Connection,
    year_month: &str,
    si: &LikeByMeSi,
) -> Result<u32, RedisError> {
    let rsp: Result<u32, redis::RedisError> = redis::cmd("vdel.si")
        .arg(format!("{},{}", si.uid, year_month))
        .arg("where")
        .arg("object_type")
        .arg("=")
        .arg(si.object_type)
        .query(conn);
    println!("cmd vdel.si rsp: {:?}", rsp);
    rsp
}

pub(super) fn aglike_cmd_vget(
    conn: &mut Connection,
    like_by_me: &LikeByMe,
) -> Result<Value, RedisError> {
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
        .query(conn);
    println!("cmd vget rsp: {:?}", rsp);
    rsp
}
