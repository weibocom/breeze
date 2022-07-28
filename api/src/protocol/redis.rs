// 通过api转换为redis协议访问

use std::io::{Error, ErrorKind, Result};

use redis::{Client, Commands, Connection};
use rocket::{serde::json::Json, Build, Rocket};

use super::{resp::Response, API_BASE};
use crate::props;

const PATH_REDIS: &str = "redis";

// redis 增加api路由接口
pub fn routes(rocket: Rocket<Build>) -> Rocket<Build> {
    rocket.mount(API_BASE, routes![get, set])
}

// 根据service获取监听端口，连接端口，进行get请求
#[get("/cmd/redis/get/<key>?<service>", format = "json")]
pub fn get(service: &str, key: &str) -> Json<Response> {
    // 统计qps
    crate::qps_incr(PATH_REDIS);

    match get_inner(service, key) {
        Ok(val) => Json(Response::from_result(val)),
        Err(e) => Json(Response::from_error(&e)),
    }
}

#[post("/cmd/redis/set/<key>?<service>", data = "<value>", format = "json")]
pub fn set(service: &str, key: &str, value: &str) -> Json<Response> {
    // 统计qps
    crate::qps_incr(PATH_REDIS);

    match set_inner(service, key, value) {
        Ok(rs) => Json(Response::from_result(rs.to_string())),
        Err(err) => Json(Response::from_error(&err)),
    }
}

fn get_inner(service: &str, key: &str) -> Result<String> {
    // 获取连接
    let mut conn = get_conn(service.to_string())?;

    // 执行get请求
    match conn.get::<&str, String>(key) {
        Ok(rs) => Ok(rs),
        Err(e) => Err(Error::new(
            ErrorKind::InvalidInput,
            format!("api redis get({}) failed:{:?}", key, e),
        )),
    }
}

fn set_inner(service: &str, key: &str, value: &str) -> Result<bool> {
    // 获取连接
    let mut conn = get_conn(service.to_string())?;

    // 执行set请求
    match conn.set::<&str, &str, bool>(key, value) {
        Ok(rs) => return Ok(rs),
        Err(e) => Err(Error::new(
            ErrorKind::Interrupted,
            format!("api redis set/{} failed:{:?}", key, e),
        )),
    }
}

fn get_conn(service: String) -> Result<Connection> {
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

    let url = format!("redis://{}", addr);
    match Client::open(url) {
        Ok(client) => match client.get_connection() {
            Ok(conn) => Ok(conn),
            Err(e) => {
                println!("found err: {:?}", e);
                return Err(Error::new(
                    ErrorKind::AddrNotAvailable,
                    format!("get conn/{} failed: {:?}", addr, e),
                ));
            }
        },
        Err(e) => {
            log::warn!("api - get redis connfailed:{:?}", e);
            return Err(Error::new(
                ErrorKind::AddrNotAvailable,
                format!("api conn to {} failed: {:?}", addr, e),
            ));
        }
    }
}
