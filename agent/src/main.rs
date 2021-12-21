use mimalloc::MiMalloc;
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

//#[global_allocator]
//static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

mod service;
use context::Context;
use crossbeam_channel::bounded;
use discovery::*;

use std::time::Duration;
use tokio::spawn;

use protocol::Result;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
//#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let ctx = Context::from_os_args();
    ctx.check()?;

    let _l = service::listener_for_supervisor(ctx.port()).await?;
    elog::init(ctx.log_dir(), &ctx.log_level)?;
    metrics::start_metric_sender(&ctx.metrics_url());
    metrics::init_local_ip(&ctx.metrics_probe);

    let discovery = Discovery::from_url(ctx.discovery());
    let (tx, rx) = bounded(128);
    discovery::dns::start_dns_resolver_refresher();
    // 启动定期更新资源配置线程
    discovery::start_watch_discovery(ctx.snapshot(), discovery, rx, ctx.tick());
    // 部分资源需要延迟drop。
    stream::start_delay_drop();

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
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}
