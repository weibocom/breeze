use context::Context;
use discovery::{Discovery, ServiceDiscovery};

use net::listener::Listener;
use std::io::Result;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::copy_bidirectional;
use tokio::spawn;
use tokio::sync::oneshot::{self, Sender};
use tokio::time::{interval_at, Instant};

#[tokio::main]
async fn main() -> Result<()> {
    coredump::register_panic_handler().unwrap();
    let ctx = Context::from_os_args();
    ctx.check()?;
    let discovery = Arc::from(Discovery::from_url(ctx.discovery()));
    let mut listeners = ctx.listeners();
    let mut tick = interval_at(
        Instant::now() + Duration::from_secs(1),
        Duration::from_secs(3),
    );
    let mut cycle = 0usize;
    loop {
        let quard = listeners.next().await;
        if quard.is_none() {
            if cycle == 0 {
                println!(
                    "all services have been processed. service configed in path:{}",
                    ctx.service_path()
                );
                cycle += 1;
            }
            tick.tick().await;
            continue;
        }
        let quard_ = quard.unwrap();
        let quard = quard_.clone();
        let discovery = Arc::clone(&discovery);
        let (tx, rx) = oneshot::channel::<bool>();
        spawn(async move {
            match process_one_service(tx, &quard, discovery).await {
                Ok(_) => println!("service listener complete address:{}", quard.address()),
                Err(e) => println!(
                    "service listener complete with error:{:?} {}",
                    e,
                    quard.address()
                ),
            };
        });
        match rx.await {
            Ok(done) => {
                if done {
                    if let Err(e) = listeners.on_listened(quard_).await {
                        println!("on listened failed:{:?}", e);
                    }
                }
            }
            Err(e) => println!("failed to recv from oneshot channel:{}", e),
        }
    }
}

async fn process_one_service(
    tx: Sender<bool>,
    quard: &context::Quadruple,
    discovery: Arc<discovery::Discovery>,
) -> Result<()> {
    let l = match Listener::bind(&quard.family(), &quard.address()).await {
        Ok(l) => {
            let _ = tx.send(true);
            l
        }
        Err(e) => {
            let _ = tx.send(false);
            return Err(e);
        }
    };
    println!("starting to serve {}", quard.address());
    let sd = Arc::new(ServiceDiscovery::new(
        discovery,
        quard.service(),
        quard.endpoint(),
        quard.snapshot(),
        quard.tick(),
    ));
    loop {
        let sd = sd.clone();
        let (client, _addr) = l.accept().await?;
        let endpoint = quard.endpoint().to_owned();
        spawn(async move {
            if let Err(e) = process_one_connection(client, sd, endpoint).await {
                println!("connection disconnected:{:?}", e);
            }
        });
    }
}

async fn process_one_connection(
    mut client: net::Stream,
    sd: Arc<ServiceDiscovery<endpoint::Topology>>,
    endpoint: String,
) -> Result<()> {
    use endpoint::Endpoint;
    let mut agent = Endpoint::from_discovery(&endpoint, sd).await?;
    copy_bidirectional(&mut client, &mut agent).await?;
    Ok(())
}
