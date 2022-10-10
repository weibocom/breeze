// 必须运行在tokio的runtime环境中
pub(super) fn start_http_server(ctx: &context::Context) {
    if cfg!(feature = "http") {
        log::info!("starting http server!");
        let mut rocket = rocket::build();
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
