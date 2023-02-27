use ds::BrzMalloc;
#[global_allocator]
static GLOBAL: BrzMalloc = BrzMalloc {};

#[macro_use]
extern crate rocket;

mod console;
mod http;
mod prometheus;
mod service;
use discovery::*;
mod init;

use ds::time::Duration;
use rt::spawn;

use protocol::Result;

// 默认支持
fn main() -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(context::get().thread_num as usize)
        .thread_name("breeze-w")
        .thread_stack_size(2 * 1024 * 1024)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async { run().await })
}

async fn run() -> Result<()> {
    let ctx = context::get();
    init::init(ctx);

    // 将dns resolver的初始化放到外层，提前进行，避免并发场景下顺序错乱 fishermen
    let discovery = discovery::Discovery::from_url(&ctx.discovery);
    let (tx, rx) = ds::chan::bounded(128);
    let snapshot = ctx.snapshot_path.to_string();
    let tick = ctx.tick();
    let mut fix = discovery::Fixed::default();
    fix.register(
        ctx.idc_path_url(),
        "",
        discovery::distance::build_refresh_idc(),
    );

    // 从vintage获取socks
    if ctx.service_pool_socks_url().len() > 1 {
        fix.register(
            ctx.service_pool_socks_url(),
            &ctx.idc,
            discovery::socks::build_refresh_socks(ctx.service_path.clone()),
        );
    } else {
        log::info!(
            "only use socks from local path: {}",
            ctx.service_path.clone()
        );
    }

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
