use crate::mc_helper::*;
use chrono::TimeZone;
use chrono_tz::Asia::Shanghai;
use endpoint::kv::uuid::*;
use memcache::MemcacheError;

#[test]
#[ignore]
fn get() {
    let client = mc_get_conn("mysql");
    let key = "3094373189550081";
    let result: Result<Option<String>, MemcacheError> = client.get(key);
    assert_eq!(true, result.expect("ok").is_none());
}

#[test]
#[ignore]
fn time_testst() {
    let id = 4839120888922294i64;
    let s = id.unix_secs();
    let display = chrono::Utc
        .timestamp_opt(s, 0)
        .unwrap()
        .with_timezone(&Shanghai)
        .format("%Y/%m/%d %H:%M")
        .to_string();

    println!("time:{}", display);
}
