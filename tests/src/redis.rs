#[cfg(test)]
mod redis_test {
    use redis::{Client, Commands};

    #[test]
    fn test_get_set() {
        println!("in redis test....");
        let client = Client::open("redis://localhost:56810").unwrap();
        let mut conn = client.get_connection().unwrap();
        let key = "k1";
        let value = "v2";

        let _: () = conn.set(&key, value).unwrap();
        match conn.get::<String, String>(key.to_string()) {
            Ok(v) => println!("get/{}, value: {}", key, v),
            Err(e) => println!("get failed, err: {:?}", e),
        }
        println!("completed redis test!");
    }
}
