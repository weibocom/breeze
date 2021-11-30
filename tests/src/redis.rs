#[cfg(test)]
mod redis_test {
    use std::collections::{HashMap, HashSet};

    use redis::{Client, Commands};

    #[test]
    fn test_get_set() {
        println!("in redis test....");
        let client_rs = Client::open("redis://localhost:56810");
        if let Err(e) = client_rs {
            println!("ignore test for connecting mesh failed!!!!!:{:?}", e);
            return;
        }
        let client = client_rs.unwrap();
        let mut conn = client.get_connection().unwrap();
        let key = "k1";
        let value = "v3";

        let _: () = conn.set(&key, value).unwrap();
        println!("redis set succeed!");
        match conn.get::<String, String>(key.to_string()) {
            Ok(v) => println!("get/{}, value: {}", key, v),
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
}
