use context::Context;
pub(crate) fn init_limit(ctx: &Context) {
    set_rlimit(ctx.no_file);
}
fn set_rlimit(no: u64) {
    // set number of file
    if let Err(_e) = rlimit::setrlimit(rlimit::Resource::NOFILE, no, no) {
        log::info!("set rlimit to {} failed:{:?}", no, _e);
    }
}
pub(crate) fn init_panic_hook() {
    use std::ops::Deref;
    std::panic::set_hook(Box::new(|panic_info| {
        let (_filename, _line) = panic_info
            .location()
            .map(|loc| (loc.file(), loc.line()))
            .unwrap_or(("<unknown>", 0));

        let _cause = panic_info
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

        log::error!("A panic occurred at {}:{}: {}", _filename, _line, _cause);
        log::error!("panic backtrace: {:?}", backtrace::Backtrace::new())
    }));
}
pub(crate) fn init_log(ctx: &Context) {
    // 提前初始化log，避免延迟导致的异常
    if let Err(e) = log::init(ctx.log_dir(), &ctx.log_level) {
        panic!("log init failed: {:?}", e);
    }
}
pub(crate) fn init_local_ip(ctx: &Context) {
    metrics::init_local_ip(&ctx.metrics_probe);
}

pub(crate) fn start_metrics_sender_task(_ctx: &Context) {
    #[cfg(feature = "graphite")]
    rt::spawn(metrics::Sender::new(
        &_ctx.metrics_url(),
        &_ctx.service_pool(),
        std::time::Duration::from_secs(10),
    ));
}
pub(crate) fn start_metrics_register_task(_ctx: &Context) {
    rt::spawn(metrics::MetricRegister::default());
}