#[cfg(test)]
mod counterservice_test {
    use rand::Rng;
    use redis::{Client, Commands, Connection};
    use std::{
        collections::{HashMap, HashSet},
        io::{Error, ErrorKind, Result},
    };

    const BASE_URL: &str = "redis://localhost:56810";
    fn rand_num() -> u64 {
        //let mut rng = rand::thread_rng();
        //rng.gen::<u64>()
        //rng.gen_range(0..18446744073709551615)
        18446744073709551
    }
    #[test]

    fn test_get() {
        println!("in counterservice get test....");
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
        println!("in counterservice set test....");
        let mut conn = get_conn()
            .map_err(|e| panic!("conn error:{:?}", e))
            .expect("conn err");
        let key = "xinxin";
        let value = rand_num();

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
    fn test_del() {
        println!("in counterservice del test....");
        let mut conn = get_conn()
            .map_err(|e| panic!("conn error:{:?}", e))
            .expect("conn err");
        let key = "xinxindel";
        let value = 456;

        let _: () = conn
            .set(key, value)
            .map_err(|e| panic!("set error:{:?}", e))
            .expect("set err");

        assert_eq!(redis::cmd("DEL").arg(key).query(&mut conn), Ok(1));

        println!("completed DEL test!");
    }
    #[test]
    fn test_exist() {
        println!("in counterservice exist test....");
        let mut conn = get_conn()
            .map_err(|e| panic!("conn error:{:?}", e))
            .expect("conn err");
        let key = "xinxindel";
        let value = 456;

        let _: () = conn
            .set(key, value)
            .map_err(|e| panic!("set error:{:?}", e))
            .expect("set err");

        assert_eq!(redis::cmd("EXISTS").arg(key).query(&mut conn), Ok(1));

        println!("completed exist!");
    }
    #[test]
    fn test_incr() {
        println!("in redis incr test....");
        let mut conn = get_conn().unwrap();
        let key = "xinxinincr";
        let value = rand_num();

        let _: () = conn
            .set(key, &value)
            .map_err(|e| panic!("set error:{:?}", e))
            .expect("set err");

        let before_val: u128 = redis::cmd("GET")
            .arg(key)
            .query(&mut conn)
            .expect("failed to before incr execute GET for 'xinxin'");
        println!("value for 'xinxin' = {:?}", before_val);

        //INCR and GET using high-level commands
        let incr: u64 = 2;
        let _: () = conn
            .incr(key, incr)
            .expect("failed to execute INCR for 'xinxin'");

        let after_val: u128 = conn
            .get(key)
            .expect("failed to after incr GET for 'xinxin'");
        println!("after incr val = {}", after_val);
        assert_eq!((before_val + incr as u128), after_val);
    }
    #[test]
    fn test_decr() {
        println!("in counterservice decr test....");
        let mut conn = get_conn().unwrap();
        let key = "xinxindecr";
        let value = rand_num() + 3;
        println!("decr val{:?}", value);

        let _: () = conn
            .set(key, value)
            .map_err(|e| panic!("set error:{:?}", e))
            .expect("set err");

        let before_val: u128 = redis::cmd("GET")
            .arg(key)
            .query(&mut conn)
            .expect("failed to before decr execute GET for 'xinxin'");
        println!("value for 'xinxin' = {}", before_val);

        let decr: u64 = 2;

        let _: () = conn
            .decr(key, decr)
            .map_err(|e| panic!(" decr error:{:?}", e))
            .expect("decr err");

        let after_val: u128 = conn
            .get(key)
            .expect("failed to after decr GET for 'xinxin'");
        println!("after decr val = {}", after_val);
        assert_eq!((before_val - decr as u128), after_val);
    }

    #[test]
    fn test_mset() {
        println!("in counterservice mset test....");
        let mut conn = get_conn()
            .map_err(|e| panic!("conn error:{:?}", e))
            .expect("conn err");

        let _: () = conn
            .set_multiple(&[
                ("xinxinmset1", 18446744073709551 as u64),
                ("xinxinmset2", 1844674407370955 as u64),
                ("xinxinmset3", 184467440737051 as u64),
            ])
            .map_err(|e| panic!("mset error:{:?}", e))
            .expect("mset err");

        assert_eq!(
            redis::cmd("GET").arg("xinxinmset1").query(&mut conn),
            Ok(18446744073709551 as u64)
        );
        assert_eq!(
            redis::cmd("GET").arg("xinxinmset2").query(&mut conn),
            Ok(1844674407370955 as u64)
        );
        assert_eq!(
            redis::cmd("GET").arg("xinxinmset3").query(&mut conn),
            Ok(184467440737051 as u64)
        );
        println!("completed counterservice mset test!");
    }

    #[test]
    fn test_countergetset() {
        println!("in counterservice mset test....");
        let mut conn = get_conn()
            .map_err(|e| panic!("conn error:{:?}", e))
            .expect("conn err");

        let _: () = conn
            .getset("getsetempty", 0)
            .map_err(|e| panic!("mset error:{:?}", e))
            .expect("mset err");
        assert_eq!(redis::cmd("GET").arg("getsetempty").query(&mut conn), Ok(0));
        assert_eq!(
            redis::cmd("GETSET")
                .arg("getsetempty")
                .arg(8)
                .query(&mut conn),
            Ok(0)
        );
        assert_eq!(redis::cmd("GET").arg("getsetempty").query(&mut conn), Ok(8));
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
