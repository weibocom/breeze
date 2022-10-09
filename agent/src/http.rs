use rocket::config::Config;
// 必须运行在tokio的runtime环境中
pub(super) fn start_http_server(ctx: &context::Context, rt: &tokio::runtime::Runtime) {
    if cfg!(feature = "http") {
        log::info!("starting http server!");
        let config = Config::figment()
            .merge(("address",&ctx.metrics_address))
            .merge(("port",&ctx.metrics_port))
            .merge(("log_level",&ctx.metrics_log_level))
            .merge(("workers",&ctx.metrics_workers));
        let rocket = rocket::custom(config);
        let rocket = crate::console::init_routes(rocket, ctx, rt);
        let rocket = crate::prometheus::init_routes(rocket);
        use rocket_async_compression::Compression;
        let rocket = rocket.attach(Compression::fairing());
        rt.spawn(async {
            if let Err(_e) = rocket.launch().await {
                log::error!("launch rocket failed: {:?}", _e);
            };
        });
    }
}
