use discovery::dns::DnsResolver;
use mimalloc::MiMalloc;
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

mod http;
mod service;
use context::Context;
use discovery::*;

use rt::spawn;
use std::time::Duration;

use protocol::Result;

use std::ops::Deref;
use std::panic;

// 默认支持
fn main() -> Result<()> {
    let ctx = Context::from_os_args();
    ctx.check()?;
    set_rlimit(ctx.no_file);
    set_panic_hook();

    // 提前初始化log，避免延迟导致的异常
    if let Err(e) = log::init(ctx.log_dir(), &ctx.log_level) {
        panic!("log init failed: {:?}", e);
    }

    let threads = ctx.thread_num as usize;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .thread_name("breeze-w")
        .thread_stack_size(2 * 1024 * 1024)
        .enable_all()
        .build()
        .unwrap();
    log::info!("runtime inited: {:?}", runtime);
    #[cfg(feature = "restful_api_enable")]
    http::start_http_server(&ctx, &runtime);
    runtime.block_on(async { run(ctx).await })
}

async fn run(ctx: Context) -> Result<()> {
    let _l = service::listener_for_supervisor(ctx.port()).await?;
    // elog::init(ctx.log_dir(), &ctx.log_level)?;
    metrics::init_local_ip(&ctx.metrics_probe);

    rt::spawn(metrics::Sender::new(
        &ctx.metrics_url(),
        &ctx.service_pool(),
        Duration::from_secs(10),
    ));
    rt::spawn(metrics::MetricRegister::default());

    // 将dns resolver的初始化放到外层，提前进行，避免并发场景下顺序错乱 fishermen
    let dns_resolver = DnsResolver::new();
    rt::spawn(discovery::dns::start_dns_resolver_refresher(dns_resolver));

    let discovery = discovery::Discovery::from_url(ctx.discovery());
    let (tx, rx) = ds::chan::bounded(128);
    let snapshot = ctx.snapshot().to_string();
    let tick = ctx.tick();
    let mut fix = discovery::Fixed::default();
    fix.register(ctx.idc_path(), discovery::distance::build_refresh_idc());
    // 从vintage获取socks
    fix.register(
        ctx.service_pool_path(),
        discovery::socks::build_refresh_socks(ctx.service_path()),
    );
    rt::spawn(watch_discovery(snapshot, discovery, rx, tick, fix));

    log::info!("server({}) inited {:?}", context::get_short_version(), ctx);

    let mut listeners = ctx.listeners();
    listeners.remove_unix_sock().await?;
    loop {
        let (quards, failed) = listeners.scan().await;

        if failed > 0 {
            metrics::set_sockfile_failed(failed);
        }
        for quard in quards {
            let discovery = tx.clone();
            spawn(async move {
                match service::process_one(&quard, discovery).await {
                    Ok(_) => log::info!("service complete:{}", quard),
                    Err(_e) => log::warn!("service failed. {} err:{:?}", quard, _e),
                }
            });
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

fn set_rlimit(no: u64) {
    // set number of file
    if let Err(_e) = rlimit::setrlimit(rlimit::Resource::NOFILE, no, no) {
        log::info!("set rlimit to {} failed:{:?}", no, _e);
    }
}
fn set_panic_hook() {
    panic::set_hook(Box::new(|panic_info| {
        let (_filename, _line) = panic_info
            .location()
            .map(|loc| (loc.file(), loc.line()))
            .unwrap_or(("<unknown>", 0));

        let _cause = panic_info
            .payload()
            .downcast_ref::<String>()
            .map(String::deref);

        let _cause = _cause.unwrap_or_else(|| {
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
