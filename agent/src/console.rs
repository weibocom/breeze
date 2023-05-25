// 必须运行在tokio的runtime环境中
#[cfg(feature = "console-api")]
pub(super) fn start_console(ctx: &context::Context) {
    log::error!("you should start console-api, this feature not supported now");
    return;
    use rocket::{
        config::{Config, Sig},
        log::LogLevel,
    };
    use std::net::Ipv4Addr;
    log::info!("starting console http server!");
    let mut c = Config::default();
    c.shutdown.ctrlc = false;
    c.shutdown.signals.insert(Sig::Hup);
    c.address = Ipv4Addr::new(0, 0, 0, 0).into();
    c.port = ctx.port;
    c.log_level = LogLevel::Critical;
    c.workers = 4;
    let rocket = rocket::custom(c);

    rt::spawn(api::start_whitelist_refresh(ctx.whitelist_host.clone()));
    let rocket = api::routes(rocket);

    //rocket = rocket.attach(rocket_async_compression::Compression::fairing());
    rt::spawn(async {
        if let Err(_e) = rocket.launch().await {
            log::error!("launch rocket failed: {:?}", _e);
        };
    });
}
