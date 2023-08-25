use context::Context;
pub(super) fn init(ctx: &Context) {
    #[cfg(feature = "panic-hook")]
    init_panic_hook();
    init_signal();
    init_limit(&ctx);
    init_log(&ctx);
    init_local_ip(&ctx);
    start_metrics_register_task(ctx);
    #[cfg(feature = "console-api")]
    crate::console::start_console(ctx);

    #[cfg(feature = "http")]
    crate::http::start(ctx);
    rt::spawn(discovery::dns::start_dns_resolver_refresher());
    crate::prometheus::register_target(ctx);
}
pub(crate) fn init_limit(ctx: &Context) {
    set_rlimit(ctx.no_file);
}
fn set_rlimit(no: u64) {
    // set number of file
    if let Err(_e) = rlimit::setrlimit(rlimit::Resource::NOFILE, no, no) {
        log::info!("set rlimit to {} failed:{:?}", no, _e);
    }
}
#[cfg(feature = "panic-hook")]
pub(crate) fn init_panic_hook() {
    use std::ops::Deref;
    std::panic::set_hook(Box::new(|panic_info| {
        let (filename, line) = panic_info
            .location()
            .map(|loc| (loc.file(), loc.line()))
            .unwrap_or(("<unknown>", 0));

        let cause = panic_info
            .payload()
            .downcast_ref::<String>()
            .map(String::deref)
            .unwrap_or_else(|| {
                panic_info
                    .payload()
                    .downcast_ref::<&str>()
                    .map(|s| *s)
                    .unwrap_or("<cause unknown>")
            });

        use std::io::Write;
        let out = &mut std::io::stderr();
        let _ = write!(out, "mesh panic {}:{}: {}\n", filename, line, cause);
        let _ = write!(out, "panic backtrace: {:?}\n", backtrace::Backtrace::new());

        log::error!("A panic occurred at {}:{}: {}", filename, line, cause);
        log::error!("panic backtrace: {:?}", backtrace::Backtrace::new())
    }));
}
pub(crate) fn init_log(ctx: &Context) {
    // 提前初始化log，避免延迟导致的异常
    if let Err(e) = log::init(&ctx.log_dir, &ctx.log_level) {
        panic!("log init failed: {:?}", e);
    }
}
pub(crate) fn init_local_ip(ctx: &Context) {
    metrics::init_local_ip(&ctx.metrics_probe);
}

pub(crate) fn start_metrics_register_task(_ctx: &Context) {
    rt::spawn(metrics::MetricRegister::default());
}

use tokio::signal::unix::{signal, SignalKind};
fn init_signal() {
    let stream = signal(SignalKind::terminate());
    match stream {
        Ok(mut stream) => {
            rt::spawn(async move {
                loop {
                    stream.recv().await;
                    println!("got signal terminate");
                }
            });
        }
        Err(e) => {
            println!("init signal failed: {:?}", e);
            return;
        }
    }
}
