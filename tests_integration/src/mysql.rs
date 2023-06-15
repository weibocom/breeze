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
    let key = "4892225613598464";
    let val = [
        -88i8, 6, -108, -120, -112, -119, -115, -90, -40, 8, -78, 6, 4, 50, 98, 98, 50, -32, 6, 0,
        -104, 7, 0, -96, 7, 0, -56, 7, 0, -48, 7, 0, -40, 7, 0, -104, 9, 1, -64, 9, 0, -56, 9, 0,
        -40, 9, 0, -32, 9, 3, -24, 9, 0, -16, 9, 0, -8, 9, 0, -128, 10, 0, -120, 10, 0, -104, 10,
        0, -14, 10, 24, 123, 34, 105, 115, 95, 115, 109, 97, 108, 108, 95, 118, 105, 100, 101, 111,
        34, 58, 102, 97, 108, 115, 101, 125, -104, 11, 0, -56, 11,
    ];
    let mut uval = [0u8; 100];
    let mut j = 0;
    for i in val {
        uval[j] = i as u8;
        j += 1;
    }
    // let val = r#"{"created_at":"","id":3503157055392381,"text":"","favorited":false,"truncated":false,"in_reply_to_status_id":"","in_reply_to_user_id":"","in_reply_to_screen_name":"","pic_ids":[],"geo":null,"mid":null,"is_show_bulletin":0,"state":0,"api_state":3,"flag":0,"list_id":0,"weiboState":0,"ip":null}"#;
    client.add(key, uval.as_ref(), 10000).unwrap();
    let result: Result<Option<Vec<u8>>, MemcacheError> = client.get(key);
    assert_eq!(uval.as_ref(), result.unwrap().unwrap());
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
