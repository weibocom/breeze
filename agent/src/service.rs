use crossbeam_channel::Sender;
use discovery::*;

use net::listener::Listener;
use std::io::{Error, ErrorKind, Result};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use stream::io::copy_bidirectional;
use tokio::spawn;

use protocol::Protocols;
// 一直侦听，直到成功侦听或者取消侦听（当前尚未支持取消侦听）
// 1. 尝试侦听之前，先确保服务配置信息已经更新完成
pub(super) async fn process_one(
    quard: &context::Quadruple,
    discovery: Sender<discovery::TopologyWriteGuard<endpoint::Topology<Protocols>>>,
    session_id: Arc<AtomicUsize>,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let parser = Protocols::try_from(&quard.protocol())?;
    let top = endpoint::Topology::try_from(parser.clone(), quard.endpoint())?;
    let (tx, rx) = discovery::topology(top, &quard.service());
    // 注册，定期更新配置
    discovery.send(tx)?;

    // 等待初始化完成
    let start = Instant::now();
    while !rx.inited() {
        if start.elapsed() >= Duration::from_secs(3) {
            log::info!("waiting inited. {} {:?}", quard, start.elapsed());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    log::info!("service inited. {} elapsed:{:?}", quard, start.elapsed());

    // 服务注册完成，侦听端口直到成功。
    while let Err(e) = _process_one(quard, parser.clone(), rx.clone(), session_id.clone()).await {
        log::warn!("service process failed. {}, err:{:?}", quard, e);
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
    Ok(())
}

async fn _process_one(
    quard: &context::Quadruple,
    parser: Protocols,
    top: discovery::TopologyReadGuard<endpoint::Topology<Protocols>>,
    session_id: Arc<AtomicUsize>,
) -> Result<()> {
    let l = Listener::bind(&quard.family(), &quard.address()).await?;

    let r_type = quard.protocol();
    let biz = quard.biz();
    let metric_id = metrics::register_name(r_type + "." + &metrics::encode_addr(&biz));
    log::info!("service started. {}", quard);

    loop {
        let top = top.clone();
        // 等待初始化成功
        let (client, _addr) = l.accept().await?;
        let endpoint = quard.endpoint().to_owned();
        let parser = parser.clone();
        let session_id = session_id.fetch_add(1, Ordering::AcqRel);
        spawn(async move {
            metrics::qps("conn", 1, metric_id);
            metrics::count("conn", 1, metric_id);
            let instant = Instant::now();
            if let Err(e) =
                process_one_connection(client, top, endpoint, parser, session_id, metric_id).await
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
pub(super) async fn listener_for_supervisor(port: u16) -> Result<TcpListener> {
    let addr = format!("127.0.0.1:{}", port);
    let l = TcpListener::bind(&addr).await?;
    Ok(l)
}
