cfg_if::cfg_if! {
if #[cfg(feature = "http")] {

use context::Context;
use rocket::{Build, Rocket};

pub(crate) fn init_routes(
    rocket: Rocket<Build>,
    ctx: &Context,
    rt: &tokio::runtime::Runtime,
) -> Rocket<Build> {
    set_env_props(ctx);
    rt.spawn(api::start_whitelist_refresh(ctx.whitelist_host.clone()));
    rocket
}
// 设置需要的变量到evns中
fn set_env_props(ctx: &context::Context) {
    let sp_name = ctx.service_path();
    let path = std::path::Path::new(&sp_name);
    let base_path = path.parent().unwrap();
    api::props::set_prop("base_path", base_path.to_str().unwrap());

    // 设置version
    api::props::set_prop("version", context::get_short_version());
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
