use std::{thread::sleep, time::Duration};

use function_name::named;
use redis::Commands;

use crate::redis_helper::get_conn;

use super::vintage::*;

// 在分片3 set key v后，更新top，交换分片2和3，那么
// key应该在分片2上
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
    create_sock(&format!(
        "127.0.0.1:8080+{}@redis:{port}@rs",
        path.replace('/', "+")
    ));
    //等待mesh初始化成功
    sleep(Duration::from_secs(10));
    let mut conn = get_conn(&format!("127.0.0.1:{port}"));
    // test_shards_4: shards 3
    assert_eq!(
        redis::cmd("hashkey").arg("test_shards_4").query(&mut conn),
        Ok(true)
    );
    assert_eq!(conn.set(argkey, 1), Ok(()));

    let config = r#"backends:
- 127.0.0.1:56378,127.0.0.1:56378
- 127.0.0.1:56379,127.0.0.1:56379
- 127.0.0.1:56381,127.0.0.1:56381
- 127.0.0.1:56380,127.0.0.1:56380
basic:
  access_mod: rw
  distribution: modula
  hash: crc32local
  listen: 56378,56379,56380,56381
  resource_type: eredis
  timeout_ms_master: 0
  timeout_ms_slave: 0
"#;
    update(path, config);
    //top更新间隔
    sleep(Duration::from_secs(30));
    assert_eq!(
        redis::cmd("hashkey").arg("test_shards_4").query(&mut conn),
        Ok(true)
    );
    assert_eq!(conn.get(argkey), Ok(false));
    // test_shards_8: shards 2
    assert_eq!(
        redis::cmd("hashkey").arg("test_shards_8").query(&mut conn),
        Ok(true)
    );
    assert_eq!(conn.get(argkey), Ok(true));
}
