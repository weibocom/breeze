// 必须运行在tokio的runtime环境中
pub(super) fn start_http_server(ctx: &context::Context) {
    if cfg!(feature = "http") {
        log::info!("starting http server!");
        let rocket = rocket::build();
        #[cfg(feature = "console-api")]
        let rocket = crate::console::init_routes(rocket, ctx);
        let rocket = crate::prometheus::init_routes(rocket);
        use rocket_async_compression::Compression;
        let rocket = rocket.attach(Compression::fairing());
        rt::spawn(async {
            if let Err(_e) = rocket.launch().await {
                log::error!("launch rocket failed: {:?}", _e);
            };
        });
    }
}
