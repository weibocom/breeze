#[cfg(test)]
mod mc_test {
    use std::collections::HashMap;

    #[test]
    fn mc_test_set() {
        let client =
            memcache::connect("memcache://10.182.1.51:20304?timeout=10&tcp_nodelay=true").unwrap();
        let key = "fooset";
        let value = "bar";
        client.add(key, "baz", 2).unwrap();
        client.set(key, value, 2).unwrap();
        let mut result: String = client.get(key).unwrap().unwrap();
        assert_eq!(result, value);
        let mut v_sizes = [4, 40, 400, 4000, 8000, 20000,1048511];
        for v_size in v_sizes {
            let val = vec![0x41; v_size];
            client
                .set(key, String::from_utf8_lossy(&val).to_string(), 2)
                .unwrap();
            result = client.get(key).unwrap().unwrap();
            assert_eq!(result, String::from_utf8_lossy(&val).to_string());
        }
        v_sizes = [4000, 40, 1048511, 4, 8000, 20000,400];
        for v_size in v_sizes {
            let val = vec![0x41; v_size];
            client
                .set(key, String::from_utf8_lossy(&val).to_string(), 2)
                .unwrap();
            result = client.get(key).unwrap().unwrap();
            assert_eq!(result, String::from_utf8_lossy(&val).to_string());
        }
        println!("completed mc set test!");
    }

    #[test]
    fn mc_test_add() {
        let client =
            memcache::connect("memcache://10.182.1.51:20304?timeout=10&tcp_nodelay=true").unwrap();
        let key = "fooadd";
        let value = "bar";
        client.add(key, value, 2).unwrap();
        let result: String = client.get(key).unwrap().unwrap();
        assert_eq!(result, value);
        println!("completed mc add test!");
    }

    #[test]
    fn mc_test_replace() {
        let client =
            memcache::connect("memcache://10.182.1.51:20304?timeout=10&tcp_nodelay=true").unwrap();
        let key = "fooreplace";
        let value = "bar";
        client.set(key, value, 3).unwrap();
        client.replace(key, "baz", 3).unwrap();
        let result: String = client.get(key).unwrap().unwrap();
        assert_eq!(result, "baz");
        println!("completed mc replace test!");
    }

    #[test]
    fn mc_test_append() {
        let client =
            memcache::connect("memcache://10.182.1.51:20304?timeout=10&tcp_nodelay=true").unwrap();
        let key = "append";
        let value = "bar";
        let append_value = "baz";
        client.set(key, value, 2).unwrap();
        client.append(key, append_value).unwrap();
        let result: String = client.get(key).unwrap().unwrap();
        assert_eq!(result, "barbaz");
        println!("completed mc append test!");
    }

    #[test]
    fn mc_test_prepend() {
        let client =
            memcache::connect("memcache://10.182.1.51:20304?timeout=10&tcp_nodelay=true").unwrap();
        let key = "prepend";
        let value = "bar";
        let append_value = "foo";
        client.set(key, value, 2).unwrap();
        client.prepend(key, append_value).unwrap();
        let result: String = client.get(key).unwrap().unwrap();
        assert_eq!(result, "foobar");
        println!("completed mc prepend test!");
    }

    #[test]
    fn mc_test_cas() {
        let client =
            memcache::connect("memcache://10.182.1.51:20304?timeout=10&tcp_nodelay=true").unwrap();
        client.set("foocas", "bar", 10).unwrap();
        let result: HashMap<String, (Vec<u8>, u32, Option<u64>)> =
            client.gets(&["foocas"]).unwrap();
        let (_, _, cas) = result.get("foocas").unwrap();
        let cas = cas.unwrap();
        assert_eq!(true, client.cas("foocas", "bar2", 10, cas).unwrap());
    }

    #[test]
    fn mc_test_gets() {
        let client =
            memcache::connect("memcache://10.182.1.51:20304?timeout=10&tcp_nodelay=true").unwrap();
        let key = "getsfoo";
        let value = "getsbar";
        client.set(key, value, 2).unwrap();
        client.set(value, key, 2).unwrap();
        let result: HashMap<String, String> = client.gets(&["getsfoo", "getsbar"]).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result["getsfoo"], "getsbar");
        assert_eq!(result["getsbar"], "getsfoo");
        println!("completed mc gets test!");
    }

    #[test]
    fn mc_test_delete() {
        let client =
            memcache::connect("memcache://10.182.1.51:20304?timeout=10&tcp_nodelay=true").unwrap();
        let key = "foo";
        let value = "bar";
        client.add(key, value, 2).unwrap();
        client.delete(key).unwrap();
        client.add(key, "foobar", 2).unwrap();
        let result: String = client.get(key).unwrap().unwrap();
        assert_eq!(result, "foobar");
        println!("completed mc delete test!");
    }

    #[test]
    fn mc_test_incr_decr() {
        let client =
            memcache::connect("memcache://10.182.1.51:20304?timeout=10&tcp_nodelay=true").unwrap();
        let key = "barr";
        client.delete(key).unwrap();

        client.set(key, 10, 3).unwrap();
        let res = client.increment(key, 5).unwrap();
        assert_eq!(res, 15);
        let res = client.decrement(key, 5).unwrap();
        assert_eq!(res, 10);
        println!("completed mc prepend test!");
    }
}