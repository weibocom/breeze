mod meta;
pub mod props;
mod protocol;

use rocket::{Build, Rocket};

#[macro_use]
extern crate rocket;

#[macro_use]
extern crate lazy_static;

use metrics::Path;

const API_PATH: &str = "api";

// 整合所有routers
pub fn routes() -> Rocket<Build> {
    let mut rocket = rocket::build();

    // 元数据相关routes
    rocket = meta::routes(rocket);

    // 各种协议 cmd相关routes
    protocol::routes(rocket)
}

// 统计
fn qps_incr(name: &'static str) {
    let mut opts = Path::new(vec![API_PATH]).qps(name);
    opts += 1;
}
