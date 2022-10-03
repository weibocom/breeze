use context::Context;
use rocket::{Build, Rocket};

pub(crate) fn init_routes(
    rocket: Rocket<Build>,
    ctx: &Context,
    rt: &tokio::runtime::Runtime,
) -> Rocket<Build> {
    #[cfg(feature = "http")]
    rt.spawn(api::start_whitelist_refresh(ctx.whitelist_host.clone()));
    rocket
}
