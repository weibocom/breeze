use context::Context;
use rocket::{Build, Rocket};

cfg_if::cfg_if! {
if #[cfg(feature = "http")] {


pub(crate) fn init_routes(
    rocket: Rocket<Build>,
    ctx: &Context,
    rt: &tokio::runtime::Runtime,
) -> Rocket<Build> {
    rt.spawn(api::start_whitelist_refresh(ctx.whitelist_host.clone()));
    rocket
}

} else {

pub(crate) fn init_routes(
    rocket: Rocket<Build>,
    _ctx: &Context,
    _rt: &tokio::runtime::Runtime,
) -> Rocket<Build> {
    rocket
}


}

}
