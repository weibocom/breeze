#[cfg(test)]
mod counterservice_test {
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};
    use redis::{Client, Commands, Connection};
    use std::collections::{HashMap, HashSet};
    // 测试端口配置了三列 repost:value为12b comment:value为10b like:value为10b
    const BASE_URL: &str = "redis://127.0.0.1:9302";
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

    //测试场景2 key的异常case

    //测试场景2 key的异常case
    // key为 long类型并且不带配置列 key为long类型并且配置列错误 非long导致的异常 为异常case
    // 异常case eg: , set 44 20 , set 44.unlike 30  set string 40
    #[test]
    // fn test_stringkey_set() {
    //     let value = 20;
    //     let key = 44.to_string();
    //     let _: () = get_conn()
    //         .set(&key, value)
    //         .map_err(|e| panic!("set error:{:?}", e))
    //         .expect("set err");
    //     assert_eq!(redis::cmd("GET").arg(key).query(&mut get_conn()), Err(e));
    // }
    // 如果value大于配置的value 为异常case
    // eg: set 4821769284223285.like  20 （一定要指定列！）
    // get 4821769284223285 =>"repost:0,comment:0,like:20"
    // get 4821769284223285.like => "20"
    #[test]
    fn test_get() {
        let key = "0.schv";

        match get_conn().get::<String, String>(key.to_string()) {
            Ok(v) => println!("get/{}, value: {}", key, v),
            Err(e) => println!("get failed, err: {:?}", e),
        }
    }

    #[test]
    fn test_del() {
        let key = "xinxindel";
        let value = 456;

        let _: () = get_conn()
            .set(key, value)
            .map_err(|e| panic!("set error:{:?}", e))
            .expect("set err");

        assert_eq!(redis::cmd("DEL").arg(key).query(&mut get_conn()), Ok(1));
    }
    #[test]
    fn test_exist() {
        let key = "xinxinexist";
        let value = 456;

        let _: () = get_conn()
            .set(key, value)
            .map_err(|e| panic!("set error:{:?}", e))
            .expect("set err");

        assert_eq!(redis::cmd("EXISTS").arg(key).query(&mut get_conn()), Ok(1));
    }
    #[test]
    fn test_incr() {
        let key = "xinxinincr";
        let value = rand_num();

        let _: () = get_conn()
            .set(key, &value)
            .map_err(|e| panic!("set error:{:?}", e))
            .expect("set err");

        let before_val: u128 = redis::cmd("GET")
            .arg(key)
            .query(&mut get_conn())
            .expect("failed to before incr execute GET for 'xinxin'");

        let incr: u32 = 2;
        let _: () = get_conn()
            .incr(key, incr)
            .expect("failed to execute INCR for 'xinxin'");

        let after_val: u128 = get_conn()
            .get(key)
            .expect("failed to after incr GET for 'xinxin'");

        assert_eq!((before_val + incr as u128), after_val);
    }
    #[test]
    fn test_decr() {
        let key = "xinxindecr";
        let value = rand_num() + 3;
        println!("decr val{:?}", value);

        let _: () = get_conn()
            .set(key, value)
            .map_err(|e| panic!("set error:{:?}", e))
            .expect("set err");

        let before_val: u128 = redis::cmd("GET")
            .arg(key)
            .query(&mut get_conn())
            .expect("failed to before decr execute GET for 'xinxin'");
        println!("value for 'xinxin' = {}", before_val);

        let decr: u32 = 2;

        let _: () = get_conn()
            .decr(key, decr)
            .map_err(|e| panic!(" decr error:{:?}", e))
            .expect("decr err");

        let after_val: u128 = get_conn()
            .get(key)
            .expect("failed to after decr GET for 'xinxin'");
        println!("after decr val = {}", after_val);
        assert_eq!((before_val - decr as u128), after_val);
    }

    #[test]
    fn test_mset() {
        let _: () = get_conn()
            .set_multiple(&[
                ("xinxinmset1", 18446744073709551 as u64),
                ("xinxinmset2", 1844674407370955 as u64),
                ("xinxinmset3", 184467440737051 as u64),
            ])
            .map_err(|e| panic!("mset error:{:?}", e))
            .expect("mset err");

        assert_eq!(
            redis::cmd("GET").arg("xinxinmset1").query(&mut get_conn()),
            Ok(18446744073709551 as u64)
        );
        assert_eq!(
            redis::cmd("GET").arg("xinxinmset2").query(&mut get_conn()),
            Ok(1844674407370955 as u64)
        );
        assert_eq!(
            redis::cmd("GET").arg("xinxinmset3").query(&mut get_conn()),
            Ok(184467440737051 as u64)
        );
    }

    #[test]
    fn test_countergetset() {
        let _: () = get_conn()
            .getset("getsetempty", 0)
            .map_err(|e| panic!("mset error:{:?}", e))
            .expect("mset err");
        assert_eq!(
            redis::cmd("GET").arg("getsetempty").query(&mut get_conn()),
            Ok(0)
        );
        assert_eq!(
            redis::cmd("GETSET")
                .arg("getsetempty")
                .arg(8)
                .query(&mut get_conn()),
            Ok(0)
        );
        assert_eq!(
            redis::cmd("GET").arg("getsetempty").query(&mut get_conn()),
            Ok(8)
        );
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

    fn get_conn() -> Connection {
        let client_rs = Client::open(BASE_URL);
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
}
