use std::{thread::sleep, time::Duration};

use function_name::named;
use redis::Commands;

use crate::redis_helper::get_conn;

use super::vintage::*;
#[test]
#[named]
fn test_refresh() {
    let argkey = function_name!();
    let path = "config/cloud/redis/testbreeze/redismeshtest_refresh";
    let config = r#"backends:
- 127.0.0.1:56378,127.0.0.1:56378
- 127.0.0.1:56379,127.0.0.1:56379
- 127.0.0.1:56380,127.0.0.1:56380
- 127.0.0.1:56381,127.0.0.1:56381
basic:
  access_mod: rw
  distribution: modula
  hash: crc32local
  listen: 56378,56379,56380,56381
  resource_type: eredis
  timeout_ms_master: 0
  timeout_ms_slave: 0
"#;
    create(path);
    update(path, config);
    let port = "56813";
    create_sock(&format!("{}@redis:{port}@rs", path.replace('/', "+")));
    //等待mesh初始化成功
    sleep(Duration::from_secs(10));
    let mut conn = get_conn(&format!("127.0.0.1:{port}"));
    assert_eq!(conn.set(argkey, 1), Ok(()));
    assert_eq!(conn.get(argkey), Ok(1));
}
