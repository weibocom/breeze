use context::Context;
use discovery::{Discovery, ServiceDiscovery};

use net::listener::Listener;
use std::io::Result;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::copy_bidirectional;
use tokio::spawn;
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
    loop {
        let quard = listeners.next().await;
        if quard.is_none() {
            println!(
                "no service found or all services have been processed. service configed in path:{}",
                ctx.service_path()
            );
            tick.tick().await;
            continue;
        }
        let quard = quard.unwrap();
        let discovery = Arc::clone(&discovery);
        spawn(async move {
            match Listener::bind(&quard.family(), &quard.address()).await {
                Ok(l) => {
                    println!("listener received:{:?}", quard);
                    let discovery = Arc::clone(&discovery);
                    let sd = Arc::new(ServiceDiscovery::new(
                        discovery,
                        quard.service(),
                        quard.snapshot(),
                        quard.tick(),
                    ));
                    loop {
                        match l.accept().await {
                            Ok((mut client, _addr)) => {
                                let sd = Arc::clone(&sd);
                                use endpoint::Endpoint;
                                let ed = Endpoint::from_discovery(&quard.endpoint(), sd);
                                match ed {
                                    Ok(mut server) => {
                                        spawn(async move {
                                            if let Err(e) =
                                                copy_bidirectional(&mut client, &mut server)
                                                    .await
                                                    .map_err(|e| {
                                                        println!(
                                                            "connection process failed:{:?}",
                                                            e
                                                        )
                                                    })
                                            {
                                                println!("error found:{:?}", e);
                                            }
                                            //println!("client closed");
                                            // TODO client closed normally
                                        });
                                    }
                                    Err(e) => {
                                        println!(
                                            "failed to establish server for the client {:?}",
                                            e
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                // TODO
                                println!("process stream failed:{:?}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    // TODO
                    println!("bind failed:{:?}", e);
                }
            }
        });
    }
    //let (_tx, rx) = oneshot::channel::<()>();
    //match rx.await {
    //    Ok(_) => {}
    //    Err(e) => {
    //        println!("failed to wait:{:?}", e);
    //    }
    //}
    //println!("never run here");
    //Ok(())
}
