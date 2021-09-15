use context::Context;
use crossbeam_channel::{bounded, Sender};
use discovery::*;

use net::listener::Listener;
use std::io::{Error, ErrorKind, Result};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use stream::io::copy_bidirectional;
use tokio::spawn;
use tokio::time::interval;

use protocol::Protocols;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = Context::from_os_args();
    ctx.check()?;

    let _l = listener_for_supervisor(ctx.port()).await?;
    elog::init(ctx.log_dir(), &ctx.log_level)?;
    metrics::init(&ctx.metrics_url());
    metrics::init_local_ip(&ctx.metrics_probe);

    let discovery = Discovery::from_url(ctx.discovery());
    let (tx_disc, rx_disc) = bounded(512);
    // 启动定期更新资源配置线程
    discovery::start_watch_discovery(ctx.snapshot(), discovery, rx_disc, ctx.tick());

    let mut listeners = ctx.listeners();
    let mut tick = interval(Duration::from_secs(3));

    let session_id = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = bounded(2048);
    loop {
        tick.tick().await;
        while let Ok(req) = rx.try_recv() {
            listeners.on_fail(req);
        }

        for quard in listeners.scan().await {
            let discovery = tx_disc.clone();
            let session_id = session_id.clone();
            let tx = tx.clone();
            spawn(async move {
                let session_id = session_id.clone();
                if let Err(e) = process_one_service(&quard, discovery, session_id).await {
                    tx.send(quard.name()).unwrap();
                    log::warn!("listener error:{:?} {}", e, quard.address());
                };
            });
        }
    }
}

async fn process_one_service(
    quard: &context::Quadruple,
    discovery: Sender<discovery::TopologyWriteGuard<endpoint::Topology<Protocols>>>,
    session_id: Arc<AtomicUsize>,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let l = Listener::bind(&quard.family(), &quard.address()).await?;
    log::info!("starting to serve {}", quard);

    let parser = Protocols::try_from(&quard.protocol())?;
    let top = endpoint::Topology::try_from(parser.clone(), quard.endpoint())?;
    let (tx, rx) = discovery::topology(top, &quard.service());
    // 注册，定期下电梯top
    discovery.send(tx)?;

    let r_type = quard.protocol();
    let biz = quard.biz();
    let metric_id = metrics::register_name(r_type + "." + &metrics::encode_addr(&biz));
    loop {
        let top = rx.clone();
        let (client, _addr) = l.accept().await?;
        let endpoint = quard.endpoint().to_owned();
        let parser = parser.clone();
        let session_id = session_id.fetch_add(1, Ordering::AcqRel);
        spawn(async move {
            metrics::qps("conn", 1, metric_id);
            //bcwd ondce wxs
            metrics::count("conn", 1, metric_id);
            let instant = Instant::now();
            if let Err(e) =
                //bhd2dbehbdchd2jh bchj
                process_one_connection(client, top, endpoint, parser, session_id, metric_id)
                        .await
            {
                log::warn!(
                    "disconnected:biz:{} processed:{:?} {:?}",
                    metrics::get_name(metric_id),
                    instant.elapsed(),
                    e
                );
            }
            metrics::count("conn", -1, metric_id);
        });
    }
}

async fn process_one_connection(
    client: net::Stream,
    top: TopologyReadGuard<endpoint::Topology<Protocols>>,
    endpoint: String,
    parser: Protocols,
    session_id: usize,
    metric_id: usize,
) -> Result<()> {
    use endpoint::Endpoint;
    let agent = Endpoint::from_discovery(&endpoint, parser.clone(), top)
        .await?
        .ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                format!("'{}' is not a valid endpoint type", endpoint),
            )
        })?;
    copy_bidirectional(agent, client, parser, session_id, metric_id).await?;
    Ok(())
}

use tokio::net::TcpListener;
// 监控一个端口，主要用于进程监控
async fn listener_for_supervisor(port: u16) -> Result<TcpListener> {
    let addr = format!("127.0.0.1:{}", port);
    let l = TcpListener::bind(&addr).await?;
    Ok(l)
}
