use std::sync::atomic::AtomicU64;

use chrono::{Days, Local};
use rand::RngCore;
use redis::{RedisError, Value};

use super::{get_conn, Mesh};

const RESTYPE: &str = "kuser";
const UID_CREATOR: AtomicU64 = AtomicU64::new(1);

#[test]
fn user_vreplace() {
    let mut conn = get_conn(&RESTYPE.get_host());
    let user = User::new();

    let rsp = add_or_replace(&mut conn, "vreplace", &user);
    println!("+++ rsp:{:?}", rsp);
    assert!(rsp.is_ok() && rsp.unwrap() >= 1);
}

#[test]
fn user_vadd() {
    let mut conn = get_conn(&RESTYPE.get_host());

    let user = User::new();

    safe_add(&mut conn, &user);
}

#[test]
fn user_vrange() {
    let mut con = get_conn(&RESTYPE.get_host());
    let user = User::new();

    safe_add(&mut con, &user);

    let rsp1 = redis::cmd("vrange")
        .arg(format!("{}", user.uid))
        .arg("field")
        .arg("uid,level,type")
        .query(&mut con);
    println!("++ vrange rsp1:{:?}", rsp1);

    let rsp2 = redis::cmd("vrange")
        .arg(format!("{}", user.uid))
        .arg("field")
        .arg("uid,level,type")
        .arg("where")
        .arg("nick")
        .arg("=")
        .arg(user.nick)
        .query(&mut con);
    println!("++ vrange rsp2:{:?}", rsp2);

    let respect = Ok(Value::Bulk(vec![
        Value::Bulk(vec![
            Value::Status("uid".to_string()),
            Value::Status("level".to_string()),
            Value::Status("type".to_string()),
        ]),
        Value::Bulk(vec![
            Value::Int(user.uid as i64),
            Value::Int(user.level as i64),
            Value::Int(user.r#type as i64),
        ]),
    ]));

    assert_eq!(rsp1, respect);
    assert_eq!(rsp2, respect);
}

#[test]
fn user_vget() {
    let mut con = get_conn(&RESTYPE.get_host());
    let user = User::new();

    safe_add(&mut con, &user);

    let rsp1 = redis::cmd("vget")
        .arg(format!("{}", user.uid))
        .arg("field")
        .arg("uid,level,type,reg_time,update_time")
        .query(&mut con);
    println!("++ vget rsp1:{:?}", rsp1);

    let respect = Ok(Value::Bulk(vec![
        Value::Bulk(vec![
            Value::Status("uid".to_string()),
            Value::Status("level".to_string()),
            Value::Status("type".to_string()),
            Value::Status("reg_time".to_string()),
            Value::Status("update_time".to_string()),
        ]),
        Value::Bulk(vec![
            Value::Int(user.uid as i64),
            Value::Int(user.level as i64),
            Value::Int(user.r#type as i64),
            Value::Data(user.reg_time.as_bytes().to_vec()),
            Value::Data(user.update_time.as_bytes().to_vec()),
        ]),
    ]));
    assert_eq!(rsp1, respect);
}

#[ignore = "util function"]
fn safe_add(conn: &mut redis::Connection, user: &User) {
    // 首先删除可能的记录
    let rsp: Result<i32, redis::RedisError> =
        redis::cmd("vdel").arg(format!("{}", user.uid)).query(conn);
    println!("+++ vdel rsp:{:?}", rsp);
    // assert_eq!(rsp, Ok(1));

    let rsp = add_or_replace(conn, "vadd", user);
    println!("+++ rsp:{:?}", rsp);
    assert_eq!(rsp, Ok(1));
}

#[ignore = "uitl fn"]
fn add_or_replace(conn: &mut redis::Connection, cmd: &str, user: &User) -> Result<i32, RedisError> {
    assert!(
        cmd.eq("vadd") || cmd.eq("vreplace"),
        "only support vadd or vreplace"
    );
    // 插入新记录
    redis::cmd(cmd)
        .arg(format!("{}", user.uid))
        .arg("level")
        .arg(user.level)
        .arg("type")
        .arg(user.r#type)
        .arg("gender")
        .arg(user.gender)
        .arg("nick")
        .arg(user.nick.clone())
        .arg("unick")
        .arg(user.unick.clone())
        .arg("domain")
        .arg(user.domain.clone())
        .arg("weihao")
        .arg(user.weihao.clone())
        .arg("iconver")
        .arg(user.iconver)
        .arg("region")
        .arg(user.region)
        .arg("privacy")
        .arg(user.privacy)
        .arg("settings")
        .arg(user.settings.clone())
        .arg("reg_ip")
        .arg(user.reg_ip)
        .arg("reg_email")
        .arg(user.reg_email.clone())
        .arg("reg_time")
        .arg(user.reg_time.clone())
        .arg("reg_source")
        .arg(user.reg_source)
        .arg("other")
        .arg(user.other.clone())
        .arg("extra")
        .arg(user.extra.clone())
        .arg("update_time")
        .arg(user.update_time.clone())
        .query(conn)
}

struct User {
    uid: u64,
    level: u8,
    r#type: u16,
    gender: u8,
    nick: String,
    unick: String,
    domain: String,
    weihao: String,
    iconver: u64,
    region: u64,
    privacy: u64,
    settings: String,
    reg_ip: i32,
    reg_email: String,
    reg_time: String,
    reg_source: u64,
    other: String,
    extra: String,
    update_time: String,
}

impl User {
    fn new() -> Self {
        let rd = rand::thread_rng().next_u64() / 10 + 1;
        let uid = rd + UID_CREATOR.fetch_add(rd, std::sync::atomic::Ordering::SeqCst);

        let now = Local::now();
        let reg_time = now
            .checked_sub_days(Days::new(1))
            .unwrap()
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();
        let update_time = now.format("%Y-%m-%d %H:%M:%S").to_string();
        println!("+++ uid:{}, reg:{}, update:{}", uid, reg_time, update_time);

        User {
            uid: uid,
            level: (uid % 10) as u8,
            r#type: (uid % 10) as u16,
            gender: (uid % 2) as u8,
            nick: format!("nick-{}", uid),
            unick: format!("unick-{}", uid),
            domain: format!("domain-{}", uid),
            weihao: format!("weihao-{}", uid),
            iconver: uid % 10000,
            region: uid % 10000,
            privacy: uid % 100,
            settings: format!("setting-{}", uid),
            reg_ip: (uid % 10000000) as i32,
            reg_email: Default::default(),
            reg_time: reg_time,
            reg_source: uid % 100000,
            other: Default::default(),
            extra: Default::default(),
            update_time: update_time,
        }
    }
}
