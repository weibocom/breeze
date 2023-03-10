use crate::mc_helper::*;
use memcache::MemcacheError;

#[test]
fn get() {
    let client = mc_get_conn("mysql");
    let key = "3094373189550081";
    let result: Result<Option<String>, MemcacheError> = client.get(key);
    assert_eq!(true, result.expect("ok").is_none());
}
