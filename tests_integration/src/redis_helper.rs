pub fn get_conn(host: &str) -> redis::Connection {
    use redis::Client;
    let host = String::from("redis://") + host;
    let client = Client::open(host).expect("get client err");
    client.get_connection().expect("get conn err")
}
