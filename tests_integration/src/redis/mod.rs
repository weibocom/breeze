//! # 已测试场景
//! ## 基本操作验证
//! ### key
//! - basic set, set ex
//! - mget 两个key, 其中只有一个set了, 预期应有一个none结果
//! - basic del
//! - basic incr
//! - 基础操作 decr, incrby, mset, exists, ttl, pttl, setnx, setex, expire, pexpire, expreat, pexpireat, persist
//! ### collection
//!  - hash基本操作hset, hsetnx, hmset, hincrby, hincrbyfloat, hdel, hget, hgetall, hlen, hkeys, hmget, hvals, hexists, hcan
//! - 地理位置相关 geoadd  geohash geopos geodist  
//!    georadius georadiusbymember存在问题
//! - list基本操作, lpush，rpush, rpushx, lpushx, linsert, lset, rpop, lpop, llen, lindex, lrange, ltrim, lrem
//! - 单个zset基本操作:
//!     zadd、zincrby、zrem、zremrangebyrank、zremrangebyscore、
//!     zremrangebylex、zrevrange、zcard、zrange、zrangebyscore、
//!     zrevrank、zrevrangebyscore、zrangebylex、zrevrangebylex、
//!     zcount、zlexcount、zscore、zscan
//! - set基本操作:
//!     sadd、smembers、srem、sismember、scard、spop、sscan
//! - list基本操作, rpush, llen, lpop, lrange, lset
//! - 单个zset基本操作, zadd, zrangebyscore withscore
//! - 单个long set基本操作, lsset, lsdump, lsput, lsgetall, lsdel, lslen, lsmexists, lsdset
//! - Bitmap基本操作:
//!     setbit、getbit、bitcount、bitpos、bitfield
//! - string基本操作:
//!     set、append、setrange、getrange、getset、strlen
//! ### conn
//!  - conn基本操作:
//!     ping、command、select、quit
//! ### 吞噬指令
//! - hashrandomq, master + hashrandomq
//! - sendtoall  sendtoallq 命令
//!## 复杂场景
//!  - set 1 1, ..., set 10000 10000等一万个key已由java sdk预先写入,
//! 从mesh读取, 验证业务写入与mesh读取之间的一致性
//! - value大小数组[4, 40, 400, 4000, 8000, 20000, 3000000],依次set后随机set,验证buffer扩容
//! - key大小数组[4, 40, 400, 4000], 依次set后get
//! - pipiline方式,set 两个key后,mget读取(注释了,暂未验证)
//! ## 非合法性指令
//! - set key, 无value; get key key 应返回错误

mod basic;

const RESTYPE: &str = "redis";
const RESTYPEWITHSLAVE: &str = "redis_with_slave";

use crate::ci::env::*;
use crate::redis_helper::*;
#[allow(unused)]
use function_name::named;
use std::vec;

//github ci 过不了,本地可以过,不清楚原因
/// pipiline方式,set 两个key后,mget读取
// #[test]
// fn test_pipeline() {
//     let mut con = get_conn(&RESTYPE.get_host());

//     let ((k1, k2),): ((i32, i32),) = redis::pipe()
//         .cmd("SET")
//         .arg("pipelinekey_1")
//         .arg(42)
//         .ignore()
//         .cmd("SET")
//         .arg("pipelinekey_2")
//         .arg(43)
//         .ignore()
//         .cmd("MGET")
//         .arg(&["pipelinekey_1", "pipelinekey_2"])
//         .query(&mut con)
//         .unwrap();

//     assert_eq!(k1, 42);
//     assert_eq!(k2, 43);
// }

/// set 1 1, ..., set 10000 10000等一万个key已由java sdk预先写入,
/// 从mesh读取, 验证业务写入与mesh读取之间的一致性
#[test]
#[cfg(feature = "github_workflow")]
fn test_get_write_by_sdk() {
    let mut con = get_conn(&RESTYPE.get_host());
    for i in exists_key_iter() {
        assert_eq!(redis::cmd("GET").arg(i).query(&mut con), Ok(i));
    }
}

///依次set [4, 40, 400, 4000, 8000, 20000, 3000000]大小的value
///验证buffer扩容,buffer初始容量4K,扩容每次扩容两倍
///后将[4, 40, 400, 4000, 8000, 20000, 3000000] shuffle后再依次set
///测试步骤:
///  1. set, key value size 4k以下，4次
///  3. set key value size 4k~8k，一次, buffer由4k扩容到8k
///  4. set key value size 8k~16k，一次，buffer在一次请求中扩容两次，由8k扩容到16k，16k扩容到32k，
///  5. set, key value size 2M以上，1次
///  6. 以上set请求乱序set一遍
#[named]
#[test]
#[cfg(feature = "github_workflow")]
fn test_set_value_fix_size() {
    let argkey = function_name!();
    let mut con = get_conn(&RESTYPE.get_host());

    let mut v_sizes = [4, 40, 400, 4000, 8000, 20000, 3000000];
    for v_size in v_sizes {
        let val = vec![1u8; v_size];
        redis::cmd("SET").arg(argkey).arg(&val).execute(&mut con);
        assert_eq!(redis::cmd("GET").arg(argkey).query(&mut con), Ok(val));
    }

    //todo random iter
    use rand::seq::SliceRandom;
    let mut rng = rand::thread_rng();
    v_sizes.shuffle(&mut rng);
    for v_size in v_sizes {
        let val = vec![1u8; v_size];
        redis::cmd("SET").arg(argkey).arg(&val).execute(&mut con);
        assert_eq!(redis::cmd("GET").arg(argkey).query(&mut con), Ok(val));
    }
}

///依次set key长度为[4, 40, 400, 4000]
#[test]
fn test_set_key_fix_size() {
    let mut con = get_conn(&RESTYPE.get_host());

    let key_sizes = [4, 40, 400, 4000];
    for key_size in key_sizes {
        let key = vec![1u8; key_size];
        redis::cmd("SET").arg(&key).arg("foo").execute(&mut con);
        assert_eq!(
            redis::cmd("GET").arg(&key).query(&mut con),
            Ok("foo".to_string())
        );
    }
}

//mget 获取10000个key
#[test]
#[cfg(feature = "github_workflow")]
fn test_mget_1000() {
    let mut con = get_conn(&RESTYPE.get_host());

    let maxkey = 1000;
    let mut keys = Vec::with_capacity(maxkey);
    for i in 1..=maxkey {
        keys.push(i);
    }
    assert_eq!(redis::cmd("MGET").arg(&keys).query(&mut con), Ok(keys));
}

// #[test]
// #[named]
// fn test_illegal() {
//     let argkey = function_name!();
//     let mut con = get_conn(&RESTYPE.get_host());

//     redis::cmd("SET")
//         .arg(argkey)
//         .query::<()>(&mut con)
//         .unwrap_err().detail()
//     redis::cmd("GET")
//         .arg(argkey)
//         .arg(argkey)
//         .query::<()>(&mut con)
//         .expect_err("get with two arg should panic");
// }
