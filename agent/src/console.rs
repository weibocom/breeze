use context::Context;
use rocket::{Build, Rocket};

#[cfg(feature = "console-api")]
pub(crate) fn init_routes(rocket: Rocket<Build>, ctx: &Context) -> Rocket<Build> {
    rt::spawn(api::start_whitelist_refresh(ctx.whitelist_host.clone()));
    let rocket = api::routes(rocket);
    rocket
}
#[cfg(not(feature = "console-api"))]
pub(crate) fn init_routes(rocket: Rocket<Build>, _ctx: &Context) -> Rocket<Build> {
    rocket
}
