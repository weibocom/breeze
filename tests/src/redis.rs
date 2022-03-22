#[cfg(test)]
mod redis_test {
    use std::{
        collections::{HashMap, HashSet},
        io::{Error, ErrorKind, Result},
    };

    use redis::{Client, Commands, Connection};

    const BASE_URL: &str = "redis://localhost:10064";

    #[test]
    fn test_get() {
        println!("in redis test....");
        let mut conn = get_conn().unwrap();

        // let key = "4711424389024351.repost";
        // let key = "100.abc";
        let key = "0.schv";

        match conn.get::<String, String>(key.to_string()) {
            Ok(v) => println!("get/{}, value: {}", key, v),
            Err(e) => println!("get failed, err: {:?}", e),
        }
        println!("completed redis test!");
    }

    #[test]
    fn test_set() {
        println!("in redis test....");
        let mut conn = get_conn().unwrap();
        let key = "k1";
        let value = "v3";

        let _: () = conn.set(key, value).unwrap();
        println!("redis set succeed!");
        match conn.get::<String, String>(key.to_string()) {
            Ok(v) => println!("get/{} succeed, value: {}", key, v),
            Err(e) => println!("get failed, err: {:?}", e),
        }
        println!("completed redis test!");
    }

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

    #[test]
    fn test_zrange() {
        print!("test zrange...");
        let mut conn = get_conn().unwrap();
        println!("++ will add and zrange!");
        let key = "k_zrange";
        let score: isize = 1;
        let field = "field_1";
        let del_rs = conn.del::<&str, i32>(key).unwrap();
        println!("del rs: {}", del_rs);
        let rs = conn.zadd::<&str, isize, &str, u64>(key, field, score);
        let rsval = rs.unwrap();
        println!("zadd rs: {}", rsval);
        let zrange_rs = conn.zrange::<&str, HashSet<String>>(key, 0, 1).unwrap();
        println!("zrange rs: {:?}", zrange_rs)
    }

    #[test]
    fn test_zrangebyscore() {
        print!("test zrangebyscore...");
        let mut conn = get_conn().unwrap();

        let key = "k_zrange";
        let score: isize = 1;
        let field = "field_1";
        let del_rs = conn.del::<&str, i32>(key).unwrap();
        println!("del rs: {}", del_rs);
        let rs = conn.zadd::<&str, isize, &str, u64>(key, field, score);
        let rsval = rs.unwrap();
        println!("zadd rs: {}", rsval);

        let rs = conn
            .zrangebyscore::<&str, f64, f64, HashSet<String>>(key, 0f64, 10f64)
            .unwrap();
        println!("zrangebyscore result: {:?}", rs);
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

    fn get_conn() -> Result<Connection> {
        let client_rs = Client::open(BASE_URL);
        if let Err(e) = client_rs {
            println!("ignore test for connecting mesh failed!!!!!:{:?}", e);
            return Err(Error::new(ErrorKind::AddrNotAvailable, "cannot get conn"));
        }
        let client = client_rs.unwrap();
        let conn = client.get_connection().unwrap();
        Ok(conn)
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
