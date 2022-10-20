use std::io::{Error, ErrorKind, Result};

use redis::{Client, Connection};

pub fn get_conn(host: &str) -> Connection {
    let host = String::from("redis://") + host;
    let client = Client::open(host);
    assert!(!client.is_err(), "get client err:{:?}", client.err());
    let client = client.unwrap();

    let conn = client.get_connection();
    assert!(!conn.is_err(), "get conn err:{:?}", conn.err());
    conn.unwrap()
}
