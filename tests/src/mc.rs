#[cfg(test)]
mod mc_test {

    use bmemcached::MemcachedClient;

    /// 测试场景：buffer扩容验证，同一个连接，同一个key, set不同大小的value
    /// 特征:    key；固定为"fooset"  value: 不同长度的String,每个字符内容为 ‘A’
    /// 
    /// 测试步骤：
    ///     <1> 建立连接，
    ///     <2> 乱序set，先set 1M的value,再乱序set其他大小的value
    ///     <3> 将set进去的value get出来，对比set进去的值与get出来的值；
    ///     <4> 由小到大分别set不同大小的value，从0开始递增，覆盖从4k->8k, 8k->32k，以及 1M(1048511 byte) 的场景，
    ///     <5> 重复步骤 <3>
    /// 
    #[test]
    fn mc_test_set() {
        let client = mc_get_conn();
        let key = "fooset";
        let mut v_sizes = [1048507, 4, 4000, 40, 8000, 20000,0,400];
        let mut result: String;
        for v_size in v_sizes {
            let val = vec![0x41; v_size];
            client
                .set(key, &String::from_utf8_lossy(&val).to_string(), 2)
                .unwrap();
            result = client.get(key).unwrap();
            println!("len is {}",String::from_utf8_lossy(&val).to_string().capacity());
            assert_eq!(result, String::from_utf8_lossy(&val).to_string());
        }
        v_sizes = [0, 4, 40, 400, 4000, 8000, 20000,1048507];
        for v_size in v_sizes {
            let val = vec![0x41; v_size];
            client
                .set(key, &String::from_utf8_lossy(&val).to_string(), 2)
                .unwrap();
            result = client.get(key).unwrap();
            assert_eq!(result, String::from_utf8_lossy(&val).to_string());
        }
        println!("completed mc set test!");
    }

   
    fn mc_get_conn() -> MemcachedClient{
        let client_rs =MemcachedClient::new(vec!["127.0.0.1:11211"], 5);
        assert_eq!(true,client_rs.is_ok());
        return client_rs.unwrap();
    }

    #[test]
    fn mc_test_add() {
        let client = mc_get_conn();
        let key = "fooadd";
        let value = "bar";
        client.add(key, value, 2).unwrap();
        let result: String = client.get(key).unwrap();
        assert_eq!(result, value);
        println!("completed mc add test!");
    }

    #[test]
    fn mc_test_replace() {
        let client = mc_get_conn();
        let key = "fooreplace";
        let value = "bar";
        client.set(key, value, 3).unwrap();
        client.replace(key, "baz", 3).unwrap();
        let result: String = client.get(key).unwrap();
        assert_eq!(result, "baz");
        println!("completed mc replace test!");
    }

    /*#[test]
    fn mc_test_append() {
        let client = mc_get_conn();
        let key = "append";
        let value = "bar";
        let append_value = "baz";
        client.set(key, value, 2).unwrap();
        client.append(key, append_value);
        let result: String = client.get(key).unwrap().unwrap();
        assert_eq!(result, "barbaz");
        println!("completed mc append test!");
    }

    #[test]
    fn mc_test_prepend() {
        let client = mc_get_conn();
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
        let client = mc_get_conn();
        client.set("foocas", "bar", 10).unwrap();
        let result: HashMap<String, (Vec<u8>, u32, Option<u64>)> =
            client.gets(&["foocas"]).unwrap();
        let (_, _, cas) = result.get("foocas").unwrap();
        let cas = cas.unwrap();
        assert_eq!(true, client.cas("foocas", "bar2", 10, cas).unwrap());
    }

    #[test]
    fn mc_test_gets() {
        let client = mc_get_conn();
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
        let client = mc_get_conn();
        let key = "foo";
        let value = "bar";
        client.add(key, value, 2).unwrap();
        client.delete(key).unwrap();
        client.add(key, "foobar", 2).unwrap();
        let result: String = client.get(key).unwrap();
        assert_eq!(result, "foobar");
        println!("completed mc delete test!");
    }

    #[test]
    fn mc_test_incr_decr() {
        let client = mc_get_conn();
        let key = "barr";
        client.delete(key).unwrap();

        client.set(key, 10, 3).unwrap();
        let res = client.increment(key, 5).unwrap();
        assert_eq!(res, 15);
        let res = client.decrement(key, 5).unwrap();
        assert_eq!(res, 10);
        println!("completed mc prepend test!");
    }*/
}