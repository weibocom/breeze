use ds::{chan::Receiver, BrzMalloc};
#[global_allocator]
static GLOBAL: BrzMalloc = BrzMalloc {};

mod console;
mod http;
mod prometheus;
mod service;
use context::Context;
use discovery::*;
mod init;

use ds::time::{sleep, Duration};
use rt::spawn;

use protocol::{Parser, Result};
use stream::{Backend, Request};
type Endpoint = Backend<Request>;
type Topology = endpoint::TopologyProtocol<Endpoint, Parser>;

// 默认支持
fn main() -> Result<()> {
    let result = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(context::get().thread_num as usize)
        .thread_name("breeze-w")
        .thread_stack_size(2 * 1024 * 1024)
        .enable_all()
        .build()
        .unwrap()
        .block_on(run());

    println!("exit {:?}", result);
    result
}

async fn run() -> Result<()> {
    let ctx = context::get();
    init::init(ctx);

    let (tx, rx) = ds::chan::bounded(128);
    discovery_init(ctx, rx).await?;

    log::info!("server inited {:?}", ctx);

    let mut listeners = ctx.listeners();
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
        sleep(Duration::from_secs(1)).await;
    }
}

async fn discovery_init(
    ctx: &'static Context,
    rx: Receiver<TopologyWriteGuard<Topology>>,
) -> Result<()> {
    // 将dns resolver的初始化放到外层，提前进行，避免并发场景下顺序错乱 fishermen
    let discovery = discovery::Discovery::from_url(&ctx.discovery);
    let snapshot = ctx.snapshot_path.to_string();
    let tick = ctx.tick();
    let mut fix = discovery::Fixed::default();

    // 首先从vintage获取socks
    let service_pool_socks_url = ctx.service_pool_socks_url();

    //watch_discovery之前执行清理sock
    let mut dir = tokio::fs::read_dir(&ctx.service_path).await?;
    while let Some(child) = dir.next_entry().await? {
        let path = child.path();
        //从vintage获取socklist，或者存在unixsock，需要事先清理
        if service_pool_socks_url.len() > 1
            || path.to_str().map(|s| s.ends_with(".sock")).unwrap_or(false)
        {
            log::info!("{:?} exists. deleting", path);
            let _ = tokio::fs::remove_file(path).await;
        }
    }

    if service_pool_socks_url.len() > 1 {
        fix.register(
            service_pool_socks_url,
            &ctx.idc,
            discovery::socks::build_refresh_socks(ctx.service_path.clone()),
        );
    }

    // 优先获取socks，再做其他操作，确保socks文件尽快构建
    fix.register(
        ctx.idc_path_url(),
        "",
        discovery::distance::build_refresh_idc(),
    );

    rt::spawn(watch_discovery(snapshot, discovery, rx, tick, fix));
    Ok(())
}
