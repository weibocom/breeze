use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

mod service;
use context::Context;
use crossbeam_channel::bounded;
use discovery::*;

use std::io::Result;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;
use tokio::spawn;

// #[tokio::main(flavor = "multi_thread", worker_threads = 5)]
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let ctx = Context::from_os_args();
    ctx.check()?;

    let _l = service::listener_for_supervisor(ctx.port()).await?;
    elog::init(ctx.log_dir(), &ctx.log_level)?;
    metrics::init(&ctx.metrics_url());
    metrics::init_local_ip(&ctx.metrics_probe);

    let discovery = Discovery::from_url(ctx.discovery());
    let (tx_disc, rx_disc) = bounded(512);
    // 启动定期更新资源配置线程
    discovery::start_watch_discovery(ctx.snapshot(), discovery, rx_disc, ctx.tick());

    let mut listeners = ctx.listeners();

    let session_id = Arc::new(AtomicUsize::new(0));
    loop {
        for quard in listeners.scan().await {
            let discovery = tx_disc.clone();
            let session_id = session_id.clone();
            spawn(async move {
                let session_id = session_id.clone();
                match service::process_one(&quard, discovery, session_id).await {
                    Ok(_) => log::info!("service complete:{}", quard),
                    Err(e) => log::warn!("service failed. {} err:{:?}", quard, e),
                }
            });
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}
