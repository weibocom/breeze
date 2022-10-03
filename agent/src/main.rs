use mimalloc::MiMalloc;
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[macro_use]
extern crate rocket;

mod console;
mod http;
mod prometheus;
mod service;
use discovery::*;
mod init;

use rt::spawn;
use std::time::Duration;

use protocol::Result;

// 默认支持
fn main() -> Result<()> {
    let ctx = context::get();
    init::init_panic_hook();
    init::init_limit(&ctx);
    init::init_log(&ctx);
    init::init_local_ip(&ctx);

    let threads = ctx.thread_num as usize;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .thread_name("breeze-w")
        .thread_stack_size(2 * 1024 * 1024)
        .enable_all()
        .build()
        .unwrap();
    log::info!("runtime inited: {:?}", runtime);
    http::start_http_server(&ctx, &runtime);
    runtime.block_on(async { run().await })
}

async fn run() -> Result<()> {
    let ctx = context::get();
    let _l = service::listener_for_supervisor(ctx.port).await?;

    // 将dns resolver的初始化放到外层，提前进行，避免并发场景下顺序错乱 fishermen
    let discovery = discovery::Discovery::from_url(&ctx.discovery);
    let (tx, rx) = ds::chan::bounded(128);
    let snapshot = ctx.snapshot_path.to_string();
    let tick = ctx.tick();
    let mut fix = discovery::Fixed::default();
    fix.register(
        ctx.idc_path.to_string(),
        discovery::distance::build_refresh_idc(),
    );

    init::start_metrics_sender_task(ctx);
    init::start_metrics_register_task(ctx);
    rt::spawn(discovery::dns::start_dns_resolver_refresher());
    rt::spawn(watch_discovery(snapshot, discovery, rx, tick, fix));

    log::info!("server inited {:?}", ctx);

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
