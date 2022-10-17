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

    use redis::{Client, Connection};

    const BASE_URL: &str = "redis://localhost:56810";

    fn get_conn() -> Result<Connection> {
        let client_rs = Client::open(BASE_URL);
        if let Err(e) = client_rs {
            println!("ignore test for connecting mesh failed!!!!!:{:?}", e);
            return Err(Error::new(ErrorKind::AddrNotAvailable, "cannot get conn"));
        }
        let client = client_rs.unwrap();
        match client.get_connection() {
            Ok(conn) => Ok(conn),
            Err(e) => {
                println!("found err: {:?}", e);
                return Err(Error::new(ErrorKind::Interrupted, e.to_string()));
            }
        }
    }

    #[test]
    fn test_args() {
        let mut con = get_conn().unwrap();

        redis::cmd("SET").arg("key1").arg(b"foo").execute(&mut con);
        redis::cmd("SET").arg(&["key2", "bar"]).execute(&mut con);

        assert_eq!(
            redis::cmd("MGET").arg(&["key1", "key2"]).query(&mut con),
            Ok(("foo".to_string(), b"bar".to_vec()))
        );
    }

    #[test]
    fn test_getset() {
        let mut con = get_conn().unwrap();

        redis::cmd("SET").arg("foo").arg(42).execute(&mut con);
        assert_eq!(redis::cmd("GET").arg("foo").query(&mut con), Ok(42));

        redis::cmd("SET").arg("bar").arg("foo").execute(&mut con);
        assert_eq!(
            redis::cmd("GET").arg("bar").query(&mut con),
            Ok(b"foo".to_vec())
        );

        let v_sizes = [5, 50, 500, 5000];
        for v_size in v_sizes {
            let val = vec![1u8; v_size];
            redis::cmd("SET").arg("bar").arg(&val).execute(&mut con);
            assert_eq!(redis::cmd("GET").arg("bar").query(&mut con), Ok(val));
        }
    }
}
