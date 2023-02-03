//! 针对连接的测试
use crate::ci::env::*;
use crate::redis::RESTYPE;
use crate::redis_helper::*;
use function_name::named;
use redis::RedisError;

/// conn基本操作:
/// ping、command、select、quit
#[named]
#[test]
fn sys_basic() {
    // hello、master 未实现
    let argkey = function_name!();
    let mut con = get_conn(&RESTYPE.get_host());
    redis::cmd("DEL").arg(argkey).execute(&mut con);

    let res: Result<String, RedisError> = redis::cmd("COMMAND").query(&mut con);
    assert_eq!(res.expect("ok"), "OK".to_string());
    let res: Result<String, RedisError> = redis::cmd("PING").query(&mut con);
    assert!(res.is_ok());
    assert_eq!(res.expect("ok"), "PONG".to_string());

    let res: Result<String, RedisError> = redis::cmd("SELECT").arg(0).query(&mut con);
    assert!(res.is_ok());
    assert_eq!(res.expect("ok"), "OK".to_string());

    assert_eq!(redis::cmd("quit").query(&mut con), Ok("OK".to_string()));
}
