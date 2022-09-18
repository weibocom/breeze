cfg_if::cfg_if! {
if #[cfg(feature = "http")] {

// 必须运行在tokio的runtime环境中
pub(super) fn start_http_server(ctx: &context::Context, rt: &tokio::runtime::Runtime) {
    log::info!("starting http server!");
    let rocket = rocket::build();
    let rocket = crate::console::init_routes(rocket, ctx, rt);
    let rocket = crate::prometheus::init_routes(rocket);
    rt.spawn(async {
        if let Err(_e) = rocket.launch().await {
            log::error!("launch rocket failed: {:?}", _e);
        };
    });
}


} else {
pub(super) fn start_http_server(_ctx: &context::Context, _rt: &tokio::runtime::Runtime) {}
}

}
