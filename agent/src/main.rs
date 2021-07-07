use context::Context;
use discovery::{Discovery, ServiceDiscovery};

use net::listener::Listener;
use std::io::{Error, ErrorKind, Result};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use stream::io::copy_bidirectional;
use tokio::spawn;
use tokio::sync::mpsc::{self, Sender};
use tokio::time::{interval_at, Instant};

use protocol::Protocols;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = Context::from_os_args();
    ctx.check()?;
    let discovery = Arc::from(Discovery::from_url(ctx.discovery()));
    let mut listeners = ctx.listeners();
    let mut tick = interval_at(
        Instant::now() + Duration::from_secs(1),
        Duration::from_secs(3),
    );
    let session_id = Arc::new(AtomicUsize::new(0));
    let (tx, mut rx) = mpsc::channel(1);
    loop {
        let quards = listeners.scan().await;
        if let Err(e) = quards {
            log::info!("scan listener failed:{:?}", e);
            tick.tick().await;
            continue;
        }
        for quard in quards.unwrap().iter() {
            let quard = quard.clone();
            let quard_ = quard.clone();
            let discovery = Arc::clone(&discovery);
            let tx = tx.clone();
            let session_id = session_id.clone();
            spawn(async move {
                let _tx = tx.clone();
                let session_id = session_id.clone();
                match process_one_service(tx, &quard, discovery, session_id).await {
                    Ok(_) => log::info!("service listener complete address:{}", quard.address()),
                    Err(e) => {
                        let _ = _tx.send(false).await;
                        log::error!("service listener error:{:?} {}", e, quard.address())
                    }
                };
            });
            if let Some(success) = rx.recv().await {
                if success {
                    if let Err(e) = listeners.on_listened(quard_).await {
                        log::error!("on_listened failed:{:?} ", e);
                    }
                }
            }
        }

        tick.tick().await;
    }
}

async fn process_one_service(
    tx: Sender<bool>,
    quard: &context::Quadruple,
    discovery: Arc<discovery::Discovery>,
    session_id: Arc<AtomicUsize>,
) -> Result<()> {
    let parser = Protocols::from(&quard.protocol()).ok_or(Error::new(
        ErrorKind::InvalidData,
        format!("'{}' is not a valid protocol", quard.protocol()),
    ))?;
    let top = endpoint::Topology::from(parser.clone(), quard.endpoint()).ok_or(Error::new(
        ErrorKind::InvalidData,
        format!("'{}' is not a valid endpoint", quard.endpoint()),
    ))?;
    let l = Listener::bind(&quard.family(), &quard.address()).await?;
    log::info!("starting to serve {}", quard.address());
    let _ = tx.send(true).await;
    let sd = Arc::new(ServiceDiscovery::new(
        discovery,
        quard.service(),
        quard.snapshot(),
        quard.tick(),
        top,
    ));
    loop {
        let sd = sd.clone();
        let (client, _addr) = l.accept().await?;
        let endpoint = quard.endpoint().to_owned();
        let parser = parser.clone();
        let session_id = session_id.fetch_add(1, Ordering::AcqRel);
        spawn(async move {
            if let Err(e) = process_one_connection(client, sd, endpoint, parser, session_id).await {
                log::warn!("connection disconnected:{:?}", e);
            }
        });
    }
}

async fn process_one_connection(
    client: net::Stream,
    sd: Arc<ServiceDiscovery<endpoint::Topology<Protocols>>>,
    endpoint: String,
    parser: Protocols,
    session_id: usize,
) -> Result<()> {
    use endpoint::Endpoint;
    let agent = Endpoint::from_discovery(&endpoint, parser.clone(), sd)
        .await?
        .ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                format!("'{}' is not a valid endpoint type", endpoint),
            )
        })?;
    copy_bidirectional(agent, client, parser, session_id).await?;
    Ok(())
}
