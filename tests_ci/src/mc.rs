/// # 已测试场景：
/// - 验证 mesh buffer 扩容，同一个连接，同一个key，set 8次不同大小的String value
///     key: "fooset"  value: 每个字符内容为 ‘A’
///     乱序set: [1048507, 4, 4000, 40, 8000, 20000, 0, 400]
///     顺序set: [0, 4, 40, 400, 4000, 8000, 20000, 1048507]
/// - 模拟mc已有10000条数据，通过 mesh 读取，验证数据一致性
///     数据由 java SDK 预先写入：key: 0...9999  value: 0...9999
/// - 模拟简单mc add命令: get -> add -> get
///     
use crate::ci::env::*;

mod mc_test {

    use bmemcached::MemcachedClient;

    use crate::ci::env::{exists_key_iter, Mesh};

    /// 测试场景：buffer扩容验证: 同一个连接，同一个key, set不同大小的value
    /// 特征:    key；固定为"fooset"  value: 不同长度的String,内容固定: 每个字符内容为 ‘A’
    ///
    /// 测试步骤：
    ///     <1> 建立连接
    ///     <2> 乱序set，先set 1M的value,再乱序set其他大小的value [1048507, 4, 4000, 40, 8000, 20000, 0, 400]
    ///     <3> 将set进去的value get出来，对比set进去的值与get出来的值；
    ///     <4> 由小到大分别set不同大小的value，从0开始递增，覆盖从4k->8k, 8k->32k，以及 1M 的场景，
    ///     <5> 重复步骤 <3>
    ///
    #[test]
    fn buffer_capacity_a() {
        let client = mc_get_conn();
        let key = "fooset";
        let mut v_sizes = [1048507, 4, 4000, 40, 8000, 20000, 0, 400];
        for v_size in v_sizes {
            let val = vec![0x41; v_size];
            assert_eq!(client
                .set(key, &String::from_utf8_lossy(&val).to_string(), 2)
                .is_ok(),true);
            let result: Result<String, bmemcached::errors::Error> = client.get(key);
            assert!(result.is_ok());
            assert_eq!(
                result.expect("ok"),
                String::from_utf8_lossy(&val).to_string()
            );
        }
        v_sizes = [0, 4, 40, 400, 4000, 8000, 20000, 1048507];
        for v_size in v_sizes {
            let val = vec![0x41; v_size];
            assert!(client
                .set(key, &String::from_utf8_lossy(&val).to_string(), 2)
                .is_ok());
            let result: Result<String, bmemcached::errors::Error> = client.get(key);
            assert!(result.is_ok());
            assert_eq!(
                result.expect("ok"),
                String::from_utf8_lossy(&val).to_string()
            );
        }
    }

    /// 测试场景：针对目前线上存在的业务方写，mesh 读数据的场景进行模拟，验证数据一致性；
    /// 特征:    预先通过java SDK 直接向后端资源写入10000条数据
    ///          key: “1”.."10000" value: 1..10000
    /// 测试步骤：根据已知的 key&value,通过 mesh 获取并对比结果
    #[test]
    fn only_get_value() {
        let client = mc_get_conn();
        let mut key: String;
        for value in exists_key_iter() {
            key = value.to_string();
            let result: Result<Vec<u8>, bmemcached::errors::Error> = client.get(key);
            assert!(result.is_ok(),"result : {:?}",result);
            assert_eq!(result.expect("ok"), value.to_string().into_bytes());
        }
    }

    fn mc_get_conn() -> MemcachedClient {
        let host = file!().get_host();
        let client_rs = MemcachedClient::new(vec![host], 5);
        assert_eq!(true, client_rs.is_ok());
        return client_rs.expect("ok");
    }

    /// 测试场景：基本的mc add 命令验证
    /// 测试步骤：get(key) -> add(key) -> get(key)
    #[test]
    fn mc_simple_add() {
        let client = mc_get_conn();
        let key = "fooadd";
        let value = "bar";
        let result: Result<String, bmemcached::errors::Error> = client.get(key);
        assert_eq!(true, result.is_err());
        client.add(key, value, 10).unwrap();
        let result: Result<String, bmemcached::errors::Error> = client.get(key);
        assert_eq!(true, result.is_ok());
        assert_eq!(result.expect("ok"), value);
    }

    /// 测试场景：基本的mc get 命令验证
    /// 测试步骤：get(key) key 已存在
    #[test]
    fn mc_simple_get() {
        let client = mc_get_conn();
        let result: Result<Vec<u8>, bmemcached::errors::Error> = client.get("4357");
        assert!(result.is_ok());
        assert_eq!(result.expect("ok"), 4357.to_string().into_bytes());
    }

    /// 测试场景：基本的mc replace 命令验证
    #[test]
    fn mc_simple_replace() {
        let client = mc_get_conn();
        let key = "fooreplace";
        let value = "bar";
        assert!(client.set(key, value, 3).is_ok());
        assert_eq!(true, client.replace(key, "baz", 3).is_ok());
        let result: Result<String, bmemcached::errors::Error> = client.get(key);
        assert_eq!(result.is_ok(), true);
        assert_eq!(result.expect("ok"), "baz");
    }

    #[test]
    fn mc_simple_delete() {
        let client = mc_get_conn();
        let key = "foodel";
        let value = "bar";
        assert!(client.add(key, value, 2).is_ok());
        assert!(client.delete(key).is_ok());
        let result: Result<String, bmemcached::errors::Error> = client.get(key);
        assert!(result.is_err());
    }

    /*
    #[test]
    fn only_set_value() {
        let client = mc_get_conn();
        let mut key: String;
        let mut number: u32 = 0;
        while number < 10000 {
            key = number.to_string();
            let result = client.set(key, number, 500);
            assert_eq!(true,result.is_ok());
            number += 1;
        }
    }

    #[test]
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
