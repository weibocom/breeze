use crate::mc_helper::*;
use chrono::TimeZone;
use chrono_tz::Asia::Shanghai;
use endpoint::mysql::uuid::*;
use memcache::MemcacheError;

#[test]
fn get() {
    let client = mc_get_conn("mysql");
    let key = "3094373189550081";
    let result: Result<Option<String>, MemcacheError> = client.get(key);
    assert_eq!(true, result.expect("ok").is_none());
}

#[test]
fn time_testst() {
    let id = 4839120888922294;
    let mills = UuidHelper::get_time(id) * 1000;
    let display = chrono::Utc
        .timestamp_millis(mills)
        .with_timezone(&Shanghai)
        .format("%Y/%m/%d %H:%M")
        .to_string();

    println!("time:{}", display);
}
