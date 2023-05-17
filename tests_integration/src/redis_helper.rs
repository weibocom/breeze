pub fn get_conn(host: &str) -> redis::Connection {
    use redis::Client;
    let hoststr = String::from("redis://") + host;
    let client = Client::open(hoststr).expect("get client err");
    client
        .get_connection()
        .expect(&("get conn err ".to_string() + host))
}
