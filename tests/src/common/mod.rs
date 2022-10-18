use std::io::{Error, ErrorKind, Result};

use redis::{Client, Connection};

pub fn get_conn(host: &str) -> Result<Connection> {
    let client_rs = Client::open(host);
    if let Err(e) = client_rs {
        println!("ignore test for connecting mesh failed!!!!!:{:?}", e);
        return Err(Error::new(ErrorKind::AddrNotAvailable, "cannot get conn"));
    }
    let client = client_rs.unwrap();
    match client.get_connection() {
        Ok(conn) => Ok(conn),
        Err(e) => {
            println!("found err: {:?}", e);
            return Err(Error::new(ErrorKind::Interrupted, e.to_string()));
        }
    }
}
