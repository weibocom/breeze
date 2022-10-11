use rocket::config::{Config, Sig};
// 必须运行在tokio的runtime环境中
pub(super) fn start_http_server(ctx: &context::Context) {
    if cfg!(feature = "http") {
        log::info!("starting http server!");
        let mut c = Config::default();
        c.shutdown.ctrlc = false;
        c.shutdown.signals.insert(Sig::Hup);
        let config = Config::figment()
            .merge(("address", "0.0.0.0"))
            .merge(("port", &ctx.port))
            .merge(("log_level", "critical"))
            .merge(("workers", 4))
            .merge(("shutdown", c.shutdown));
        let mut rocket = rocket::custom(config);
        if cfg!(feature = "console-api") {
            rocket = crate::console::init_routes(rocket, ctx);
        }
        rocket = crate::prometheus::init_routes(rocket);
        rocket = rocket.attach(rocket_async_compression::Compression::fairing());
        rt::spawn(async {
            if let Err(_e) = rocket.launch().await {
                log::error!("launch rocket failed: {:?}", _e);
            };
        });
    }
}
