use context::Context;
use discovery::{Discovery, ServiceDiscovery};

use net::listener::Listener;
use std::io::Result;
use std::sync::Arc;

use tokio::io::copy_bidirectional;
use tokio::spawn;
use tokio::sync::oneshot;

#[tokio::main]
async fn main() -> Result<()> {
    coredump::register_panic_handler().unwrap();
    let ctx = Context::from_os_args();
    ctx.check()?;
    let discovery = Arc::from(Discovery::from_url(ctx.discovery()));
    for quard in ctx.listeners() {
        let discovery = Arc::clone(&discovery);
        spawn(async move {
            match Listener::bind(&quard.family(), &quard.address()).await {
                Ok(l) => {
                    println!("listener received:{:?}", quard);
                    let discovery = Arc::clone(&discovery);
                    let sd = ServiceDiscovery::new(discovery, quard.service());
                    loop {
                        match l.accept().await {
                            Ok((mut client, _addr)) => {
                                let sd = Arc::clone(&sd);
                                use endpoint::Endpoint;
                                let ed = Endpoint::from_discovery(&quard.endpoint(), sd).await;
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
    let (_tx, rx) = oneshot::channel::<()>();
    match rx.await {
        Ok(_) => {}
        Err(e) => {
            println!("failed to wait:{:?}", e);
        }
    }
    println!("never run here");
    Ok(())
}
