#![cfg(feature = "http")]

use super::prometheus::prometheus_metrics;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use rt::spawn;

pub(crate) fn start(ctx: &context::Context) {
    let addr = ([0, 0, 0, 0], ctx.port).into();

    let service = make_service_fn(|_| async { Ok::<_, hyper::Error>(service_fn(route)) });
    let server = Server::bind(&addr).serve(service);
    spawn(async {
        if let Err(_e) = server.await {
            log::error!("hyper failed: {:?}", _e);
        };
    });
}

async fn route(req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/metrics") => prometheus_metrics().await,
        _ => {
            let mut not_found = Response::default();
            *not_found.status_mut() = StatusCode::NOT_FOUND;
            Ok(not_found)
        }
    }
}
