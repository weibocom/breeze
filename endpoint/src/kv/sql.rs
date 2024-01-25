#[rustfmt::skip]
macro_rules! sql {
    (add) => {"insert into {} (id,content) values ({},'{}')"};
    (set) => {"update {} set content='{}' where id={}"};
    (del) => {"delete from {} where id={}"};
    (get) => {"select content from {} where id={}"};
}

macro_rules! sql_len {
    ($op:expr, $table:expr, $id:expr, $value:expr) => {{
        let len = match $op {
            0 => sql!(add).len(),
            1 => sql!(set).len(),
            2 => sql!(del).len(),
            3 => sql!(get).len(),
            _ => panic!("not support op:{}", $op),
        };
        len + $table.len() + $id.len() + $value.map(|v| v.len()).unwrap_or(0)
    }};
}

macro_rules! sql_write {
    ($op:expr, $w:expr, $table:expr, $id:expr, $value:expr) => {
        match $op {
            0 => write!($w, sql!(add), $table, $id, $value.unwrap()),
            1 => write!($w, sql!(set), $table, $id, $value.unwrap()),
            2 => write!($w, sql!(del), $table, $id),
            3 => write!($w, sql!(get), $table, $id),
            _ => panic!("not support op:{}", $op),
        }
    };
}

fn _check() {
    use std::io::Write;
    let _l = sql_len!(0, "db.table", "id", Some("value"));
    let mut v: Vec<u8> = Vec::new();
    let _r = sql_write!(0, &mut v, "db.table", "id", Some("value"));
    let _r = sql_write!(3, &mut v, "db.table", "id", None::<String>);
}
