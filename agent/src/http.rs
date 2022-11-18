use rocket::{
    config::{Config, Sig},
    log::LogLevel,
};
use std::net::Ipv4Addr;
// 必须运行在tokio的runtime环境中
pub(super) fn start_http_server(ctx: &context::Context) {
    if cfg!(feature = "http") {
        log::info!("starting http server!");
        let mut c = Config::default();
        c.shutdown.ctrlc = false;
        c.shutdown.signals.insert(Sig::Hup);
        c.address = Ipv4Addr::new(0, 0, 0, 0).into();
        c.port = ctx.port;
        c.log_level = LogLevel::Critical;
        c.workers = 4;
        let mut rocket = rocket::custom(c);
        rocket = crate::console::init_routes(rocket, ctx);
        rocket = crate::prometheus::init_routes(rocket);
        //rocket = rocket.attach(rocket_async_compression::Compression::fairing());
        rt::spawn(async {
            if let Err(_e) = rocket.launch().await {
                log::error!("launch rocket failed: {:?}", _e);
            };
        });
    }
}
