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
use futures_util::stream::StreamExt;
use signal_hook::consts::signal::*;
use signal_hook::low_level::raise;
use signal_hook_tokio::Signals;

async fn handle_signals(mut signals: Signals, metrics_url: String, service_pool: String) {
    let mut sender = metrics::Sender::new(
        metrics_url.as_str(),
        service_pool.as_str(),
        Duration::from_secs(10),
    );
    while let Some(signal) = signals.next().await {
        match signal {
            SIGILL | SIGABRT | SIGFPE | SIGSEGV => {
                log::error!("coredump backtrace: {:?}", Backtrace::new());
                let noop = noop_waker::noop_waker();
                let mut ctx = std::task::Context::from_waker(&noop);
                let mut retry_times = 0;
                while sender.poll_send_coredump(&mut ctx).is_pending() {
                    retry_times += 1;
                    if retry_times > 3 {
                        break;
                    }
                }
                raise(signal).unwrap();
            }
            _ => unreachable!(),
        }
    }
}
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
//#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let signals = Signals::new(&[SIGILL, SIGABRT, SIGFPE, SIGSEGV])?;
    let _handle = signals.handle();
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
    let ctx = Context::from_os_args();
    ctx.check()?;
    // set number of file
    if let Err(e) = rlimit::setrlimit(rlimit::Resource::NOFILE, ctx.no_file, ctx.no_file) {
        log::info!("set rlimit to {} failed:{:?}", ctx.no_file, e);
    }

    let _l = service::listener_for_supervisor(ctx.port()).await?;
    elog::init(ctx.log_dir(), &ctx.log_level)?;

    // 在专用线程中初始化4个定时任务。
    metrics::init_local_ip(&ctx.metrics_probe);
    let mut spawner = rt::DedicatedSpawner::new();
    let metric_cycle = Duration::from_secs(10);
    spawner.spawn(metrics::Sender::new(
        &ctx.metrics_url(),
        &ctx.service_pool(),
        metric_cycle,
    ));
    spawner.spawn(metrics::MetricRegister::default());
    spawner.spawn(discovery::dns::start_dns_resolver_refresher());
    spawner.spawn(handle_signals(
        signals,
        ctx.metrics_url().clone(),
        ctx.service_pool().clone(),
    ));
    use discovery::watch_discovery;
    let discovery = Discovery::from_url(ctx.discovery());
    let (tx, rx) = ds::chan::bounded(128);
    let snapshot = ctx.snapshot().to_string();
    let tick = ctx.tick();
    let mut fix = discovery::Fixed::default();
    fix.register(ctx.idc_path(), sharding::build_refresh_idc());
    spawner.spawn(watch_discovery(snapshot, discovery, rx, tick, fix));
    spawner.spawn(stream::start_delay_drop());
    log::info!("starting a dedicated thread for periodic tasks");
    spawner.start_on_dedicated_thread();

    // 启动定期更新资源配置线程
    // 部分资源需要延迟drop。

    log::info!("====> server inited <====");

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
