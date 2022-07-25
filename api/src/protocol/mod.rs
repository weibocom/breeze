use rocket::{Build, Rocket};

mod memcache;
mod phantom;
mod redis;
mod resp;

pub const API_BASE: &str = "/breeze";

// redis 增加api路由接口
pub fn routes(rocket: Rocket<Build>) -> Rocket<Build> {
    let rocket = redis::routes(rocket);
    phantom::routes(rocket)
}
