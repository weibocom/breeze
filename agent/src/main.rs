use discovery::dns::DnsResolver;
use mimalloc::MiMalloc;
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

mod service;
use context::Context;
use discovery::*;

use rt::spawn;
use std::time::Duration;

use protocol::Result;

use std::ops::Deref;
use std::panic;
use std::path::Path;

use backtrace::Backtrace;

#[cfg(feature = "restful_api_enable")]
#[macro_use]
extern crate rocket;

#[cfg(feature = "restful_api_enable")]
use rocket::{Build, Rocket};

use api;
use api::props;

// 默认支持
#[cfg(not(feature = "restful_api_enable"))]
fn main() -> Result<()> {
    let ctx = Context::from_os_args();
    ctx.check()?;
    set_rlimit(ctx.no_file);
    set_panic_hook();

    // 提前初始化log，避免延迟导致的异常
    if let Err(e) = elog::init(ctx.log_dir(), &ctx.log_level) {
        panic!("log init failed: {:?}", e);
    }

    log::info!("launch without rocket!");
    let threads = ctx.thread_num as usize;
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .thread_name("breeze-w")
        .thread_stack_size(2 * 1024 * 1024)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async { run(ctx).await })
}

// 支持 restful api 的启动入口
// 注意：api所有逻辑只在api module中进行实现，不要扩散到其他mod fishermen
#[cfg(feature = "restful_api_enable")]
#[launch]
async fn main_launch() -> Rocket<Build> {
    let ctx = Context::from_os_args();
    if let Err(e) = ctx.check() {
        panic!("context check args failed, err: {:?}", e);
    }
    set_rlimit(ctx.no_file);
    set_panic_hook();

    // 提前初始化log，避免延迟导致的异常
    if let Err(e) = elog::init(ctx.log_dir(), &ctx.log_level) {
        panic!("log init failed: {:?}", e);
    }

    // set env props for api
    set_env_props(&ctx);

    // 启动api的白名单探测
    rt::spawn(api::start_whitelist_refresh(ctx.whitelist_host.clone()));

    // 启动核心任务
    rt::spawn(async {
        if let Err(e) = run(ctx).await {
            panic!("start breeze core failed: {:?}", e);
        }
    });

    log::info!("launch with rocket!");

    api::routes()
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
    fix.register(ctx.idc_path(), sharding::build_refresh_idc());
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
                    Err(e) => log::warn!("service failed. {} err:{:?}", quard, e),
                }
            });
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

fn set_rlimit(no: u64) {
    // set number of file
    if let Err(e) = rlimit::setrlimit(rlimit::Resource::NOFILE, no, no) {
        log::info!("set rlimit to {} failed:{:?}", no, e);
    }
}
fn set_panic_hook() {
    panic::set_hook(Box::new(|panic_info| {
        let (filename, line) = panic_info
            .location()
            .map(|loc| (loc.file(), loc.line()))
            .unwrap_or(("<unknown>", 0));

        let cause = panic_info
            .payload()
            .downcast_ref::<String>()
            .map(String::deref);

        let cause = cause.unwrap_or_else(|| {
            panic_info
                .payload()
                .downcast_ref::<&str>()
                .map(|s| *s)
                .unwrap_or("<cause unknown>")
        });

        log::error!("A panic occurred at {}:{}: {}", filename, line, cause);
        log::error!("panic backtrace: {:?}", Backtrace::new())
    }));
}

// 设置需要的变量到evns中
pub fn set_env_props(ctx: &Context) {
    let sp_name = ctx.service_path();
    let path = Path::new(&sp_name);
    let base_path = path.parent().unwrap();
    props::set_prop("base_path", base_path.to_str().unwrap());

    // 设置version
    props::set_prop("version", context::get_short_version());
}
