#[cfg(test)]
mod counterservice_test {
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};
    use redis::{Client, Commands, Connection};
    use std::{
        collections::{HashMap, HashSet},
        io::{Error, ErrorKind, Result},
    };

    const BASE_URL: &str = "redis://localhost:56810";
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
    //已经完成 set get getset del incr decr mset exist命令的测试
    //eg 21596端口配置 value-size 4字节 key-size 8字节
    //set key 和value不能为空 空会异常
    //set 的key有 随机字母数字组合+{.,_,_*_}dsg
    //eg:0FIkJm2PkzOidMU9wdJINYRTATJSO8.dsg
    // .  0FIkJm2PkzOidMU9wdJINYRTATJSO8_dsg
    // .  0FIkJm2PkzOidMU9wdJINYRTATJSO8_dsg_dsg
    //纯数字  eg:2222222。dsg
    //纯字母 eg:rtyuioiuy.dsg
    //VALUE最大为32bit 因为是计数器所以value一定是数字 如果value为非数字异常
    //过程：set 上述覆盖的key 用随机32b作为val
    //再get 做断言 判断get到的val和set进去的val是否相等
    //结束
    #[test]
    fn test_counterserviceset() {
        let mut conn = get_conn()
            .map_err(|e| panic!("conn error:{:?}", e))
            .expect("conn err");

        let mut key_onlynum = rand_num().to_string();
        key_onlynum.push_str(".msg");
        let mut key_onlyletter: String = thread_rng()
            .sample_iter(Alphanumeric)
            .take(7)
            .map(char::from)
            .collect();
        key_onlyletter.push_str("_dsg");
        let keys = vec![
            rand_key(".dsg"),
            rand_key("_dsg"),
            rand_key("_dsg_abc"),
            key_onlyletter,
            key_onlynum,
        ];
        for key in keys.iter() {
            let value = rand_num();
            let _: () = conn
                .set(key, value)
                .map_err(|e| panic!("set error:{:?}", e))
                .expect("set err");
            assert_eq!(redis::cmd("GET").arg(key).query(&mut conn), Ok(value));
        }
    }
    #[test]
    fn test_get() {
        let mut conn = get_conn()
            .map_err(|e| panic!("conn error:{:?}", e))
            .expect("conn err");

        let key = "0.schv";

        match conn.get::<String, String>(key.to_string()) {
            Ok(v) => println!("get/{}, value: {}", key, v),
            Err(e) => println!("get failed, err: {:?}", e),
        }
    }

    #[test]
    fn test_del() {
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
    }
    #[test]
    fn test_exist() {
        let mut conn = get_conn()
            .map_err(|e| panic!("conn error:{:?}", e))
            .expect("conn err");
        let key = "xinxinexist";
        let value = 456;

        let _: () = conn
            .set(key, value)
            .map_err(|e| panic!("set error:{:?}", e))
            .expect("set err");

        assert_eq!(redis::cmd("EXISTS").arg(key).query(&mut conn), Ok(1));
    }
    #[test]
    fn test_incr() {
        let mut conn = get_conn()
            .map_err(|e| panic!("conn error:{:?}", e))
            .expect("conn err");
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

        let incr: u32 = 2;
        let _: () = conn
            .incr(key, incr)
            .expect("failed to execute INCR for 'xinxin'");

        let after_val: u128 = conn
            .get(key)
            .expect("failed to after incr GET for 'xinxin'");

        assert_eq!((before_val + incr as u128), after_val);
    }
    #[test]
    fn test_decr() {
        let mut conn = get_conn()
            .map_err(|e| panic!("conn error:{:?}", e))
            .expect("conn err");
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

        let decr: u32 = 2;

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
    }

    #[test]
    fn test_countergetset() {
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
        let client = client_rs
            .map_err(|e| panic!("client error:{:?}", e))
            .expect("client err");
        match client.get_connection() {
            Ok(conn) => Ok(conn),
            Err(e) => {
                println!("found err: {:?}", e);
                return Err(Error::new(ErrorKind::Interrupted, e.to_string()));
            }
        }
    }
}
