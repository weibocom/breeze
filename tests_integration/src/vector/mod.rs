use crate::ci::env::*;
use crate::redis_helper::*;
#[allow(unused)]
use function_name::named;

const RESTYPE: &str = "vector";

#[test]
#[named]
fn vrange_basic() {
    let argkey = function_name!();
    let mut con = get_conn(&RESTYPE.get_host());

    let rsp = redis::cmd("vrange")
        .arg(format!("{argkey},2105"))
        .arg("field")
        .arg("a,b")
        .arg("where")
        .arg("a")
        .arg("=")
        .arg("1")
        .arg("b")
        .arg("in")
        .arg("2,3")
        .arg("order")
        .arg("a")
        .arg("desc")
        .arg("limit")
        .arg("12")
        .arg("24")
        .query(&mut con);
    assert_eq!(rsp, Ok(32));
}
