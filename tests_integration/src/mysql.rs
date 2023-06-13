use crate::mc_helper::*;
// use chrono::TimeZone;
// use chrono_tz::Asia::Shanghai;
// use endpoint::kv::uuid::*;
use memcache::MemcacheError;

#[test]
#[ignore]
fn get() {
    let client = mc_get_conn("mysql");
    let key = "4892225613598471";
    let result: Result<Option<String>, MemcacheError> = client.get(key);
    println!("{:?}", result);
    assert_eq!(true, result.expect("ok").is_none());
}

#[test]
fn set() {
    let client = mc_get_conn("mysql");
    let key = "3503157055392381";
    let val = r#"{"created_at":"","id":3503157055392381,"text":"","favorited":false,"truncated":false,"in_reply_to_status_id":"","in_reply_to_user_id":"","in_reply_to_screen_name":"","pic_ids":[],"geo":null,"mid":null,"is_show_bulletin":0,"state":0,"api_state":3,"flag":0,"list_id":0,"weiboState":0,"ip":null}"#;
    client.add(key, val, 10000).unwrap();
    let result: Result<Option<String>, MemcacheError> = client.get(key);
    println!("{:?}", result);
    assert_eq!(val, result.unwrap().unwrap());
}

// #[test]
// fn time_testst() {
//     let id = 4839120888922294i64;
//     let s = id.unix_secs();
//     let display = chrono::Utc
//         .timestamp_opt(s, 0)
//         .unwrap()
//         .with_timezone(&Shanghai)
//         .format("%Y/%m/%d %H:%M")
//         .to_string();

//     println!("time:{}", display);
// }
