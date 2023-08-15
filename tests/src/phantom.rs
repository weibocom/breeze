#[cfg(test)]
mod phantom_test {
    use ahash::{HashMap, HashMapExt};
    use std::{
        collections::HashSet,
        io::{Error, ErrorKind, Result},
    };

    use redis::{Client, Commands, Connection};

    const BASE_URL: &str = "redis://localhost:10041";

    fn pfget() {
        let conn = get_conn().unwrap();
        let key = "12345678.1234567890";
        conn.pfadd(key, element)
    }

    fn get_conn() -> Result<Connection> {
        let client_rs = Client::open(BASE_URL);
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
}
