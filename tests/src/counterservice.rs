#[cfg(test)]
mod counterservice_test {
    use std::{
        collections::{HashMap, HashSet},
        io::{Error, ErrorKind, Result},
    };

    use redis::{Client, Commands, Connection};

    const BASE_URL: &str = "redis://localhost:10052";

    #[test]

    fn test_get() {
        println!("in redis test....");
        let mut conn = get_conn().unwrap();

        // let key = "4711424389024351.repost";
        // let key = "100.abc";
        let key = "0.schv";
        // let key = "4743334465374053.read";

        match conn.get::<String, String>(key.to_string()) {
            Ok(v) => println!("get/{}, value: {}", key, v),
            Err(e) => println!("get failed, err: {:?}", e),
        }
        println!("completed redis test!");
    }

    #[test]
    fn test_set() {
        println!("in redis test....");
        let mut conn = get_conn()
            .map_err(|e| panic!("conn error:{:?}", e))
            .expect("conn err");
        let key = "4711424389024351.repost";
        let value = "v3";

        let _: () = conn
            .set(key, value)
            .map_err(|e| panic!("set error:{:?}", e))
            .expect("set err");

        // if let Err(e) == Rsult {
        //     assert
        // }
        println!("redis set succeed!");
        match conn.get::<String, String>(key.to_string()) {
            Ok(v) => println!("get/{} succeed, value: {}", key, v),
            Err(e) => println!("get failed, err: {:?}", e),
        }
        println!("completed redis test!");
    }

    #[test]
    fn test_incr() {
        println!("in redis incr test....");
        let mut conn = get_conn().unwrap();
        let key = "xinxin";

        let before_val: i32 = redis::cmd("GET")
            .arg(key)
            .query(&mut conn)
            .expect("failed to before incr execute GET for 'xinxin'");
        println!("value for 'xinxin' = {}", before_val);

        //INCR and GET using high-level commands
        let incr = 2;
        let _: () = conn
            .incr(key, incr)
            .expect("failed to execute INCR for 'xinxin'");

        let after_val: i32 = conn
            .get(key)
            .expect("failed to after incr GET for 'xinxin'");
        println!("after incr val = {}", after_val);
        assert_eq!(before_val + incr, after_val);
    }
    #[test]
    fn test_decr() {
        println!("in redis decr test....");
        let mut conn = get_conn().unwrap();
        let key = "xinxin";

        let before_val: i32 = redis::cmd("GET")
            .arg(key)
            .query(&mut conn)
            .expect("failed to before decr execute GET for 'xinxin'");
        println!("value for 'xinxin' = {}", before_val);

        //INCR and GET using high-level commands
        let decr = 2;
        let _: () = conn
            .decr(key, decr)
            .expect("failed to execute DECR for 'xinxin'");

        let after_val: i32 = conn
            .get(key)
            .expect("failed to after decr GET for 'xinxin'");
        println!("after decr val = {}", after_val);
        assert_eq!(before_val - decr, after_val);
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
        match client.get_connection() {
            Ok(conn) => Ok(conn),
            Err(e) => {
                println!("found err: {:?}", e);
                return Err(Error::new(ErrorKind::Interrupted, e.to_string()));
            }
        }
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
