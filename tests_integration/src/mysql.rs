use crate::mc_helper::*;
use memcache::MemcacheError;

//val中有非assic字符和需要mysql转义的字符
#[test]
fn set() {
    let client = mc_get_conn("mysql");
    let key = "4892225613598465";
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

#[test]
fn update() {
    let client = mc_get_conn("mysql");
    let key = "4892225613598454";

    client.add(key, "1", 10000).unwrap();
    let result: Result<Option<String>, MemcacheError> = client.get(key);
    assert_eq!("1", result.unwrap().unwrap());

    client.set(key, "2", 10000).unwrap();
    let result: Result<Option<String>, MemcacheError> = client.get(key);
    assert_eq!("2", result.unwrap().unwrap());
}

#[test]
fn delete() {
    let client = mc_get_conn("mysql");
    let key = "4892225613598453";

    client.add(key, "1", 10000).unwrap();
    assert_eq!("1", client.get::<String>(key).unwrap().unwrap());

    client.delete(key).unwrap();
    assert_eq!(None, client.get::<String>(key).unwrap());
}
