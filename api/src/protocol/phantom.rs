use std::{io::Result, net::IpAddr};

use redis::{cmd, Client, Connection};
use rocket::{serde::json::Json, Build, Rocket};
use std::io::{Error, ErrorKind};

use super::resp::Response;
use crate::{props, verify_client};

const PATH_PHANTOM: &str = "phantom";

// phantom 增加路由
pub fn routes(rocket: Rocket<Build>) -> Rocket<Build> {
    rocket.mount(super::API_BASE, routes![bfget, bfset])
}

#[get("/cmd/phantom/bfget/<key>?<service>", format = "json")]
pub fn bfget(service: &str, key: &str, cip: IpAddr) -> Json<Response> {
    // 校验client
    if !verify_client(&cip.to_string(), PATH_PHANTOM) {
        return Json(Response::from_illegal_user());
    }

    match bfget_inner(service, key) {
        Ok(rs) => Json(Response::from_result(rs)),
        Err(err) => Json(Response::from_error(&err)),
    }
}

#[post("/cmd/phantom/bfset/<key>?<service>", format = "json")]
pub fn bfset(service: &str, key: &str, cip: IpAddr) -> Json<Response> {
    // 校验client
    if !verify_client(&cip.to_string(), PATH_PHANTOM) {
        return Json(Response::from_illegal_user());
    }

    match bfset_inner(service, key) {
        Ok(rs) => Json(Response::from_result(rs)),
        Err(err) => Json(Response::from_error(&err)),
    }
}

fn bfget_inner(service: &str, key: &str) -> Result<String> {
    let mut conn = get_conn(service.to_string())?;

    match cmd("bfget").arg(key).query::<i64>(&mut conn) {
        Ok(rs) => Ok(format!("{}", rs)),
        Err(err) => Err(Error::new(
            ErrorKind::Other,
            format!("api bfget({}) failed:{:?}", key, err),
        )),
    }
}

fn bfset_inner(service: &str, key: &str) -> Result<String> {
    let mut conn = get_conn(service.to_string())?;

    match cmd("bfset").arg(key).query::<i64>(&mut conn) {
        Ok(rs) => Ok(format!("{}", rs)),
        Err(err) => Err(Error::new(
            ErrorKind::Other,
            format!("api bfset({}) failed: {:?}", key, err),
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
