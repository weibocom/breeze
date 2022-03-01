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

use backtrace::Backtrace;

fn main() -> Result<()> {
    let ctx = Context::from_os_args();
    ctx.check()?;
    set_rlimit(ctx.no_file);
    set_panic_hook();

    let threads = ctx.thread_num as usize;
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .thread_name("breeze")
        .thread_stack_size(2 * 1024 * 1024)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async { run(ctx).await })
}

async fn run(ctx: Context) -> Result<()> {
    let _l = service::listener_for_supervisor(ctx.port()).await?;
    elog::init(ctx.log_dir(), &ctx.log_level)?;

    // 在专用线程中初始化4个定时任务。
    metrics::init_local_ip(&ctx.metrics_probe);
    let mut spawner = rt::DedicatedSpawner::new();
    spawner.spawn(metrics::Sender::new(
        &ctx.metrics_url(),
        &ctx.service_pool(),
        Duration::from_secs(10),
    ));
    spawner.spawn(metrics::MetricRegister::default());
    spawner.spawn(discovery::dns::start_dns_resolver_refresher());
    let discovery = discovery::Discovery::from_url(ctx.discovery());
    let (tx, rx) = ds::chan::bounded(128);
    let snapshot = ctx.snapshot().to_string();
    let tick = ctx.tick();
    let mut fix = discovery::Fixed::default();
    fix.register(ctx.idc_path(), sharding::build_refresh_idc());
    spawner.spawn(watch_discovery(snapshot, discovery, rx, tick, fix));
    //spawner.spawn(stream::start_delay_drop());
    spawner.start_on_dedicated_thread();

    // 启动定期更新资源配置线程
    // 部分资源需要延迟drop。

    log::info!("====> server inited <==== {:?}", ctx);

    let mut listeners = ctx.listeners();
    loop {
        for quard in listeners.scan().await {
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
