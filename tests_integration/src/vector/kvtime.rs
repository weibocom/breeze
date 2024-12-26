use redis::{RedisError, Value};
use crate::redis_helper::*;
use super::super::mysql::MAX_PAYLOAD_LEN;

// Invalid arg异常参数对应的响应
const ERR_INVALID_REQ: &str = "invalid request";
const VALUE_FIELD_NAME: &str = "content";
const KEY_FIELD_NAME: &str = "id";

fn setget(key: &str, val: &str) -> Result<(), RedisError> {
    let mut conn = get_conn("kv2vector");
    let rsp: Value = redis::cmd("vadd")
        .arg(key)
        .arg(VALUE_FIELD_NAME)
        .arg(val)
        .query(&mut conn)?;
    assert_eq!(rsp, Value::Int(1));

    let rsp: Value = redis::cmd("vrange")
        .arg(key)
        .query(&mut conn)?;
    assert_eq!(rsp, redis::Value::Bulk(vec![
        Value::Bulk(vec![
            Value::Data(KEY_FIELD_NAME.into()),
            redis::Value::Data(VALUE_FIELD_NAME.into()),
        ]),
        redis::Value::Bulk(vec![
            redis::Value::Data(key.into()),
            redis::Value::Data(val.into()),
        ]),
    ]));

    Ok(())
}
#[test]
#[rustfmt::skip]
fn set() -> Result<(), RedisError> {
    let key = "4892225613598165";
    let val = "4892225613598165_val";
    setget(key, val)
}

#[test]
fn update() -> Result<(), RedisError> {
    let key = "4892225613598154";
    let val = "4892225613598154_val";
    let _ = setget(key, val)?;
    let val = "4892225613598154_val2";
    let mut conn = get_conn("kv2vector");
    let rsp: Value = redis::cmd("vupdate")
        .arg(key)
        .arg(VALUE_FIELD_NAME)
        .arg(val)
        .query(&mut conn)?;
    assert_eq!(rsp, Value::Int(1));

    let rsp: Value = redis::cmd("get")
        .arg(key)
        .query(&mut conn)?;
    assert_eq!(rsp, redis::Value::Bulk(vec![
        Value::Bulk(vec![
            Value::Data(KEY_FIELD_NAME.into()),
            redis::Value::Data(VALUE_FIELD_NAME.into()),
        ]),
        redis::Value::Bulk(vec![
            redis::Value::Data(key.into()),
            redis::Value::Data(val.into()),
        ]),
    ]));

    Ok(())
}

#[test]
fn delete() -> Result<(), RedisError> {
    let key = "4892225613598153";
    let val = "4892225613598153_val";
    let mut conn = get_conn("kv2vector");
    let rsp: Value = redis::cmd("vadd")
        .arg(key)
        .arg(VALUE_FIELD_NAME)
        .arg(val)
        .query(&mut conn)?;
    assert_eq!(rsp, Value::Int(1));

    let rsp: Value = redis::cmd("vdel")
        .arg(key)
        .query(&mut conn)?;
    assert_eq!(rsp, Value::Int(1));

    Ok(())
}

//构建一个sql长度为MAX_PAYLOAD_LEN的packet
#[test]
fn set_huge_payload() -> Result<(), RedisError> {
    let key = "4892225613598123";
    //当val长度为MAX_PAYLOAD_LEN - 1 - 76，构建出来的insert语句长度恰好为MAX_PAYLOAD_LEN
    let val = vec!['a' as u8; MAX_PAYLOAD_LEN];
    let mut conn = get_conn("kv2vector");
    let rsp: Value = redis::cmd("vadd")
        .arg(key)
        .arg(VALUE_FIELD_NAME)
        .arg(val)
        .query(&mut conn)?;
    assert_eq!(rsp, Value::Int(1));
    Ok(())
}

#[test]
fn update_not_exsit() -> Result<(), RedisError> {
    let key = "4892225613598145";
    let mut conn = get_conn("kv2vector");
    let _: Value = redis::cmd("vdel")
        .arg(key)
        .query(&mut conn)?;
    let rsp: Value = redis::cmd("vupdate")
        .arg(key)
        .arg(VALUE_FIELD_NAME)
        .arg("4892225613598145_val")
        .query(&mut conn)?;
    assert_eq!(rsp, Value::Int(0));
    Ok(())
}

#[test]
fn delete_not_exsit()-> Result<(), RedisError> {
    let mut conn = get_conn("kv2vector");
    let key = "4892225613598146";
    let _: Value = redis::cmd("vdel")
        .arg(key)
        .query(&mut conn)?;

    let rsp: Value = redis::cmd("vdel")
        .arg(key)
        .query(&mut conn)?;
    assert_eq!(rsp, Value::Int(0));
    Ok(())
}

#[test]
fn get_invalid_key() -> Result<(), RedisError> {
    let mut conn = get_conn("kv2vector");
    let result: Result<Value, RedisError> = redis::cmd("vget")
        .arg("9527")
        .query(&mut conn);
    assert!(result.is_err_and(|e| e.to_string().contains(ERR_INVALID_REQ)));
    Ok(())
}

#[test]
fn insert_invalid_key() -> Result<(), RedisError> {
    let key = "9527";
    let mut conn = get_conn("kv2vector");
    let result: Result<Value, RedisError>  = redis::cmd("vadd")
        .arg(key)
        .arg(VALUE_FIELD_NAME)
        .arg("abcd")
        .query(&mut conn);
    assert!(result.is_err_and(|e| e.to_string().contains(ERR_INVALID_REQ)));
    Ok(())
}


#[test]
fn insert_char_key() -> Result<(), RedisError> {
    let key = "48922256135981cc";
    let mut conn = get_conn("kv2vector");
    let result: Result<Value, RedisError>  = redis::cmd("vadd")
        .arg(key)
        .arg(VALUE_FIELD_NAME)
        .arg("abcd")
        .query(&mut conn);
    assert!(result.is_err_and(|e| e.to_string().contains(ERR_INVALID_REQ)));
    Ok(())
}
#[test]
fn insert_long_key()-> Result<(), RedisError> {
    let key = std::iter::repeat('5').take(1024).collect::<String>();
    let mut conn = get_conn("kv2vector");
    let result: Result<Value, RedisError>  = redis::cmd("vadd")
        .arg(key)
        .arg(VALUE_FIELD_NAME)
        .arg("abcd")
        .query(&mut conn);
    assert!(result.is_err_and(|e| e.to_string().contains(ERR_INVALID_REQ)));
    Ok(())
}

#[test]
fn update_invalid_key() -> Result<(), RedisError> {
    let mut conn = get_conn("kv2vector");
    let key = "9527";
    let result: Result<Value, RedisError>  = redis::cmd("vupdate")
        .arg(key)
        .arg(VALUE_FIELD_NAME)
        .arg("abcd")
        .query(&mut conn);
    assert!(result.is_err_and(|e| e.to_string().contains(ERR_INVALID_REQ)));
    Ok(())
}

#[test]
fn update_char_key() -> Result<(), RedisError> {
    let mut conn = get_conn("kv2vector");
    let key = "48922256135981cc";
    let result: Result<Value, RedisError>  = redis::cmd("vupdate")
        .arg(key)
        .arg(VALUE_FIELD_NAME)
        .arg("abcd")
        .query(&mut conn);
    assert!(result.is_err_and(|e| e.to_string().contains(ERR_INVALID_REQ)));
    Ok(())
}
#[test]
fn update_long_key() -> Result<(), RedisError> {
    let key = std::iter::repeat('5').take(1024).collect::<String>();
    let mut conn = get_conn("kv2vector");
    let result: Result<Value, RedisError>  = redis::cmd("vupdate")
        .arg(key)
        .arg(VALUE_FIELD_NAME)
        .arg("abcd")
        .query(&mut conn);
    assert!(result.is_err_and(|e| e.to_string().contains(ERR_INVALID_REQ)));
    Ok(())
}

#[test]
fn delete_invalid_key() -> Result<(), RedisError> {
    let key = "9527";
    let mut conn = get_conn("kv2vector");
    let result: Result<Value, RedisError>  = redis::cmd("vdel")
        .arg(key)
        .query(&mut conn);
    assert!(result.is_err_and(|e| e.to_string().contains(ERR_INVALID_REQ)));
    Ok(())
}
#[test]
fn delete_char_key() -> Result<(), RedisError> {
    let mut conn = get_conn("kv2vector");
    let key = "48922256135981cc";
    let result: Result<Value, RedisError>  = redis::cmd("vdel")
        .arg(key)
        .query(&mut conn);
    assert!(result.is_err_and(|e| e.to_string().contains(ERR_INVALID_REQ)));
    Ok(())
}
#[test]
fn delete_long_key() -> Result<(), RedisError> {
    let mut conn = get_conn("kv2vector");
    let key = std::iter::repeat('5').take(1024).collect::<String>();
    let result: Result<Value, RedisError>  = redis::cmd("vdel")
        .arg(key)
        .query(&mut conn);
    assert!(result.is_err_and(|e| e.to_string().contains(ERR_INVALID_REQ)));
    Ok(())
}
#[test]
#[should_panic]
#[ignore]
fn sql_inject_get()  {
    let mut conn = get_conn("kv2vector");
    let key_inject = "4892225613598265 or 1=1 --";
    let _result:Result<Value, RedisError> = redis::cmd("vget")
        .arg(key_inject)
        .query(&mut conn);
}
