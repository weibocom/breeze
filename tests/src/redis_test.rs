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

    use crate::common::*;
    use rand::seq::SliceRandom;
    use redis::{Client, Connection};

    const BASE_URL: &str = "redis://localhost:56810";

    #[test]
    fn test_args() {
        let mut con = get_conn(BASE_URL).unwrap();

        redis::cmd("SET").arg("key1").arg(b"foo").execute(&mut con);
        redis::cmd("SET").arg(&["key2", "bar"]).execute(&mut con);

        assert_eq!(
            redis::cmd("MGET").arg(&["key1", "key2"]).query(&mut con),
            Ok(("foo".to_string(), b"bar".to_vec()))
        );
    }

    #[test]
    fn test_getset() {
        let mut con = get_conn(BASE_URL).unwrap();

        redis::cmd("SET").arg("foo").arg(42).execute(&mut con);
        assert_eq!(redis::cmd("GET").arg("foo").query(&mut con), Ok(42));

        redis::cmd("SET").arg("bar").arg("foo").execute(&mut con);
        assert_eq!(
            redis::cmd("GET").arg("bar").query(&mut con),
            Ok(b"foo".to_vec())
        );
        let v_sizes = [4, 40, 400, 4000, 8000, 20000, 3000000];
        for v_size in v_sizes {
            let val = vec![1u8; v_size];
            redis::cmd("SET").arg("bar").arg(&val).execute(&mut con);
            assert_eq!(redis::cmd("GET").arg("bar").query(&mut con), Ok(val));
        }
        //todo random iter
        use rand::seq::SliceRandom;
        let mut rng = rand::thread_rng();
        for v_size in v_sizes.shuffle(&mut rng) {
            let val = vec![1u8; v_size];
            redis::cmd("SET").arg("bar").arg(&val).execute(&mut con);
            assert_eq!(redis::cmd("GET").arg("bar").query(&mut con), Ok(val));
        }
    }
}
