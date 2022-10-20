use crate::ci::env::Mesh;
use assert_panic::assert_panic;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use redis::{Client, Commands, Connection};
use std::collections::{HashMap, HashSet};

use crate::ci::env::exists_key_iter;

fn rand_num() -> u32 {
    let mut rng = rand::thread_rng();
    rng.gen::<u32>()
    //rng.gen_range(0..18446744073709551615)
    //18446744073709551
}
fn rand_key(tail: &str) -> String {
    let mut rng = thread_rng();
    let mut s: String = (&mut rng)
        .sample_iter(Alphanumeric)
        .take(5)
        .map(char::from)
        .collect();
    s.push_str(tail);
    s
}

fn record_max_key() {}

// set 1 2, ..., set 10000 10000等一万个key已由java sdk预先写入,
// set 1.repost 1 set 1.comment 1 . set 1.like.1
// 从mesh读取, 验证业务写入与mesh读取之间的一致性
#[test]
fn test_consum_diff_write() {
    let column_cfg = vec![".repost", ".comment", ".like"];
    for column in column_cfg.iter() {
        for i in exists_key_iter() {
            let mut key = i.to_string();
            key.push_str(column);
            assert_eq!(redis::cmd("GET").arg(key).query(&mut get_conn()), Ok(i));
        }
    }
}
// 测试场景1 key为long类型并且带配置列
// 特征：key为纯数字long类型 key指定为44
// 测试端口配置了三列 repost comment like
// set 44.repost 20 , set 44.comment 20   , set 44.like 20

//过程：建立连接
//轮询向指定列里发送 value为20 的key
//再get 做断言 判断get到的val和set进去的val是否相等
#[test]
fn test_sample_set() {
    let column_cfg = vec![".repost", ".comment", ".like"];
    for column in column_cfg.iter() {
        let value = 20;
        let mut key = 44.to_string();
        key.push_str(column);

        let _: () = get_conn()
            .set(&key, value)
            .map_err(|e| panic!("set error:{:?}", e))
            .expect("set err");

        assert_eq!(redis::cmd("GET").arg(key).query(&mut get_conn()), Ok(value));
    }
}

// 测试场景2：在测试1的基础上 让下一个key的大小比当前已知最大key大20 和大10000000000（1e10）
// 已知配置中max-diff为1000000000 （1e9）
// 对该key的数据类型范围进行限制之外，还需要检测该key减去所要存储的table中的当前现有最大key的差值，这个差值不应超过max-diff
// 特征:已知当前max-key为4821769284223285 现在set  key1=max-key+20    key2=max-key+1e10 value为20
// key1:set 4821769284223305.like 20    key2:set 4821779284223285.like 20

//流程
// 分别向like列 发送value为20的key1和key2
// 在get key1是否为20  如果key2返回key too big 测试通过

#[test]
fn test_key_maxdiff_set() {
    let value = 20;
    let key1 = "4821769284223305.like";
    let key2 = "4821779284223285.like";

    let _: () = get_conn()
        .set(key1, value)
        .map_err(|e| panic!("set error:{:?}", e))
        .expect("set err");
    assert_eq!(
        redis::cmd("GET").arg(key1).query(&mut get_conn()),
        Ok(value)
    );

    assert_panic!(panic!( "{:?}", get_conn().set::<String, u8, String>(key2.to_string(), value)), String, contains "key too big");
}

// 测试场景3 key的异常case
// key为 long类型但是不带配置列  提示Invalid key
// key为long类型并且配置列错误  提示No schema
// key为string类型非long导致的异常  提示nvalid key
// 异常case eg:  set 44 20 , set 44.unlike 30  set like 40
#[test]
fn test_diffkey_set() {
    let value = 20;

    let key_nocolumn = 44;
    assert_panic!(panic!( "{:?}", get_conn().set::<u8, u8, String>(key_nocolumn, value)), String, contains "Invalid key");

    let key_errcolumn = "44.unlike";
    assert_panic!(panic!( "{:?}", get_conn().set::<String, u8, String>(key_errcolumn.to_string(), value)), String, contains "No schema");

    let key_string = "like";
    assert_panic!(panic!( "{:?}", get_conn().set::<String, u8, String>(key_string.to_string(), value)), String, contains "Invalid key");
}

// get key=> 得到所有列的值
// get 64 =>"repost:30,comment:30,like:30"
// get key的指定列=>获取一个value
// get 64.like => "30"

//流程
//先对每一列set value为20 key为64
//再get key.column
//再get key
#[test]
fn test_sample_get() {
    let column_cfg = vec![".repost", ".comment", ".like"];
    let value = 30;
    let key = 64;
    for column in column_cfg.iter() {
        let mut key_column = key.to_string();
        key_column.push_str(column);

        let _: () = get_conn()
            .set(&key_column, value)
            .map_err(|e| panic!("set error:{:?}", e))
            .expect("set err");

        assert_eq!(
            redis::cmd("GET").arg(key_column).query(&mut get_conn()),
            Ok(value)
        );
    }

    assert_eq!(
        redis::cmd("GET").arg(key).query(&mut get_conn()),
        Ok(String::from("repost:30,like:30,comment:30"))
    );
}
// todo:如果value大于配置的value 为异常case
// 测试端口配置了三列 repost:value为12b comment:value为10b like:value为10b

// #[test]
// fn test_del() {
//     let key = "xinxindel";
//     let value = 456;

//     let _: () = get_conn()
//         .set(key, value)
//         .map_err(|e| panic!("set error:{:?}", e))
//         .expect("set err");

//     assert_eq!(redis::cmd("DEL").arg(key).query(&mut get_conn()), Ok(1));
// }
// #[test]
// fn test_exist() {
//     let key = "xinxinexist";
//     let value = 456;

//     let _: () = get_conn()
//         .set(key, value)
//         .map_err(|e| panic!("set error:{:?}", e))
//         .expect("set err");

//     assert_eq!(redis::cmd("EXISTS").arg(key).query(&mut get_conn()), Ok(1));
// }
// #[test]
// fn test_incr() {
//     let key = "xinxinincr";
//     let value = rand_num();

//     let _: () = get_conn()
//         .set(key, &value)
//         .map_err(|e| panic!("set error:{:?}", e))
//         .expect("set err");

//     let before_val: u128 = redis::cmd("GET")
//         .arg(key)
//         .query(&mut get_conn())
//         .expect("failed to before incr execute GET for 'xinxin'");

//     let incr: u32 = 2;
//     let _: () = get_conn()
//         .incr(key, incr)
//         .expect("failed to execute INCR for 'xinxin'");

//     let after_val: u128 = get_conn()
//         .get(key)
//         .expect("failed to after incr GET for 'xinxin'");

//     assert_eq!((before_val + incr as u128), after_val);
// }
// #[test]
// fn test_decr() {
//     let key = "xinxindecr";
//     let value = rand_num() + 3;
//     println!("decr val{:?}", value);

//     let _: () = get_conn()
//         .set(key, value)
//         .map_err(|e| panic!("set error:{:?}", e))
//         .expect("set err");

//     let before_val: u128 = redis::cmd("GET")
//         .arg(key)
//         .query(&mut get_conn())
//         .expect("failed to before decr execute GET for 'xinxin'");
//     println!("value for 'xinxin' = {}", before_val);

//     let decr: u32 = 2;

//     let _: () = get_conn()
//         .decr(key, decr)
//         .map_err(|e| panic!(" decr error:{:?}", e))
//         .expect("decr err");

//     let after_val: u128 = get_conn()
//         .get(key)
//         .expect("failed to after decr GET for 'xinxin'");
//     println!("after decr val = {}", after_val);
//     assert_eq!((before_val - decr as u128), after_val);
// }

// #[test]
// fn test_mset() {
//     let _: () = get_conn()
//         .set_multiple(&[
//             ("xinxinmset1", 18446744073709551 as u64),
//             ("xinxinmset2", 1844674407370955 as u64),
//             ("xinxinmset3", 184467440737051 as u64),
//         ])
//         .map_err(|e| panic!("mset error:{:?}", e))
//         .expect("mset err");

//     assert_eq!(
//         redis::cmd("GET").arg("xinxinmset1").query(&mut get_conn()),
//         Ok(18446744073709551 as u64)
//     );
//     assert_eq!(
//         redis::cmd("GET").arg("xinxinmset2").query(&mut get_conn()),
//         Ok(1844674407370955 as u64)
//     );
//     assert_eq!(
//         redis::cmd("GET").arg("xinxinmset3").query(&mut get_conn()),
//         Ok(184467440737051 as u64)
//     );
// }

// #[test]
// fn test_countergetset() {
//     let _: () = get_conn()
//         .getset("getsetempty", 0)
//         .map_err(|e| panic!("mset error:{:?}", e))
//         .expect("mset err");
//     assert_eq!(
//         redis::cmd("GET").arg("getsetempty").query(&mut get_conn()),
//         Ok(0)
//     );
//     assert_eq!(
//         redis::cmd("GETSET")
//             .arg("getsetempty")
//             .arg(8)
//             .query(&mut get_conn()),
//         Ok(0)
//     );
//     assert_eq!(
//         redis::cmd("GET").arg("getsetempty").query(&mut get_conn()),
//         Ok(8)
//     );
// }

#[test]
fn test_hosts_eq() {
    let hosts1 = create_hosts();
    let hosts2 = create_hosts();
    if hosts1.eq(&hosts2) {
        println!("hosts are equal!");
    } else {
        assert!(false);
    }
}

fn create_hosts() -> HashMap<String, HashSet<String>> {
    let mut hosts = HashMap::with_capacity(3);
    for i in 1..10 {
        let h = format!("{}-{}", "host", i);
        let mut ips = HashSet::with_capacity(5);
        for j in 1..20 {
            let ip = format!("ip-{}", j);
            ips.insert(ip);
        }
        hosts.insert(h.clone(), ips);
    }
    hosts
}

fn get_conn() -> Connection {
    let host = file!().get_host();
    let host_clinet = String::from("redis://") + &host;

    let client_rs = Client::open(host_clinet);
    assert!(!client_rs.is_err(), "get client err:{:?}", client_rs.err());

    let client = client_rs
        .map_err(|e| panic!("client error:{:?}", e))
        .expect("client err");
    let conn = client
        .get_connection()
        .map_err(|e| panic!("get_conn() error:{:?}", e))
        .expect("get_conn() err");
    conn
}
