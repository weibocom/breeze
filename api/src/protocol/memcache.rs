// memcache api访问接口，目前支持binary client

use std::io::{Error, ErrorKind, Result};

use memcache::Client;
use rocket::{serde::json::Json, Build, Rocket};

use super::resp::Response;
use crate::props;

const PATH_MC: &str = "memcache";

// memcache 增加路由
pub fn routes(rocket: Rocket<Build>) -> Rocket<Build> {
    rocket.mount(super::API_BASE, routes![get, set])
}

#[get("/cmd/memcache/get/<key>?<service>", format = "json")]
pub fn get(service: &str, key: &str) -> Json<Response> {
    // 统计qps
    crate::qps_incr(PATH_MC);

    match get_inner(service, key) {
        Ok(val) => Json(Response::from_result(val)),
        Err(err) => Json(Response::from_error(&err)),
    }
}

#[post(
    "/cmd/memcache/set/<key>?<service>&<expiration>",
    data = "<value>",
    format = "json"
)]
pub fn set(service: &str, key: &str, value: &str, expiration: u32) -> Json<Response> {
    // 统计qps
    crate::qps_incr(PATH_MC);

    match set_inner(service, key, value, expiration) {
        Ok(rs) => Json(Response::from_result(format!("{}", rs))),
        Err(err) => Json(Response::from_error(&err)),
    }
}

fn get_inner(service: &str, key: &str) -> Result<String> {
    let client = get_client(service)?;
    match client.get::<String>(key) {
        Ok(val) => match val {
            Some(v) => Ok(format!("{}", v)),
            None => Ok(format!("{}", " ")),
        },
        Err(err) => Err(Error::new(
            ErrorKind::InvalidData,
            format!("api mc get({}) failed: {:?}", key, err),
        )),
    }
}

// expiration key/value 过期时间，单位second
fn set_inner(service: &str, key: &str, value: &str, expiration: u32) -> Result<bool> {
    let client = get_client(service)?;
    match client.set(key, value, expiration) {
        Ok(()) => Ok(true),
        Err(err) => Err(Error::new(
            ErrorKind::Other,
            format!("api mc set/{} failed:{:?}", key, err),
        )),
    }
}

fn get_client(service: &str) -> Result<Client> {
    // 根据service获得监听的listener
    let addr = match props::get_listener(&service) {
        Some(addr) => addr,
        None => {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("not found listener for service:{}", service),
            ));
        }
    };

    // 对于文本协议i： protocol=ascii
    let url = format!("memcache://{}?protocol=binary", addr);

    match memcache::connect(url) {
        Ok(cli) => Ok(cli),
        Err(err) => Err(Error::new(
            ErrorKind::ConnectionRefused,
            format!("connect mc/{} failed: {:?}", service, err),
        )),
    }
}
