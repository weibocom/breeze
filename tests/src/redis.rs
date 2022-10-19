/// # 已测试场景
/// - basic set
/// - value大小数组[4, 40, 400, 4000, 8000, 20000, 3000000],依次set后随机set,验证buffer扩容
/// - basic del
/// - basic incr

#[cfg(test)]
mod redis_test {
    use std::collections::{HashMap, HashSet};
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

    #[test]
    fn hash_find() {
        let key = "测试123.key";
        let idx = key.find(".").unwrap();
        println!(". is at: {}", idx);
        for p in 0..idx {
            println!("{}:{}", p, key.as_bytes()[p] as char);
        }
        println!("\r\nhash find test ok!");
    }
}

#[cfg(test)]
mod redis_intergration_test {
    use std::io::{Error, ErrorKind, Result};

    use crate::redis_helper::*;

    const BASE_URL: &str = "redis://localhost:56810";

    //基本场景
    #[test]
    fn test_args() {
        let mut con = get_conn(BASE_URL);

        redis::cmd("SET")
            .arg("key1args")
            .arg(b"foo")
            .execute(&mut con);
        redis::cmd("SET")
            .arg(&["key2args", "bar"])
            .execute(&mut con);

        assert_eq!(
            redis::cmd("MGET")
                .arg(&["key1args", "key2args"])
                .query(&mut con),
            Ok(("foo".to_string(), b"bar".to_vec()))
        );
    }

    //基本set场景，key固定为foo或bar，value为简单数字或字符串
    #[test]
    fn test_basic_set() {
        let mut con = get_conn(BASE_URL);

        redis::cmd("SET").arg("fooset").arg(42).execute(&mut con);
        assert_eq!(redis::cmd("GET").arg("fooset").query(&mut con), Ok(42));

        redis::cmd("SET").arg("barset").arg("foo").execute(&mut con);
        assert_eq!(
            redis::cmd("GET").arg("barset").query(&mut con),
            Ok(b"foo".to_vec())
        );
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
    #[test]
    fn test_set_value_fix_size() {
        let mut con = get_conn(BASE_URL);

        let mut v_sizes = [4, 40, 400, 4000, 8000, 20000, 3000000];
        for v_size in v_sizes {
            let val = vec![1u8; v_size];
            redis::cmd("SET")
                .arg("bar_set_value_size")
                .arg(&val)
                .execute(&mut con);
            assert_eq!(
                redis::cmd("GET").arg("bar_set_value_size").query(&mut con),
                Ok(val)
            );
        }

        //todo random iter
        use rand::seq::SliceRandom;
        let mut rng = rand::thread_rng();
        v_sizes.shuffle(&mut rng);
        for v_size in v_sizes {
            let val = vec![1u8; v_size];
            redis::cmd("SET")
                .arg("bar_set_value_size")
                .arg(&val)
                .execute(&mut con);
            assert_eq!(
                redis::cmd("GET").arg("bar_set_value_size").query(&mut con),
                Ok(val)
            );
        }
    }

    #[test]
    fn test_basic_del() {
        let mut con = get_conn(BASE_URL);

        redis::cmd("SET").arg("foodel").arg(42).execute(&mut con);

        assert_eq!(redis::cmd("GET").arg("foodel").query(&mut con), Ok(42));

        redis::cmd("DEL").arg("foodel").execute(&mut con);

        assert_eq!(
            redis::cmd("GET").arg("foodel").query(&mut con),
            Ok(None::<usize>)
        );
    }

    #[test]
    fn test_basic_incr() {
        let mut con = get_conn(BASE_URL);

        redis::cmd("SET").arg("fooincr").arg(42).execute(&mut con);
        assert_eq!(
            redis::cmd("INCR").arg("fooincr").query(&mut con),
            Ok(43usize)
        );
    }
}
