use metrics::prometheus::Prometheus;

use ds::lock::Lock;
use ds::time::{interval, timeout, Duration, Instant};
use hyper::{header::CONTENT_TYPE, Body, Client, Request, Response, StatusCode};
use lazy_static::lazy_static;
use tokio_util::io::ReaderStream;

lazy_static! {
    static ref LAST: Lock<Instant> = Instant::now().into();
}

pub async fn prometheus_metrics() -> Result<Response<Body>, hyper::Error> {
    let mut rsp = Response::default();
    if let Ok(mut last) = LAST.try_lock() {
        let secs = last.elapsed().as_secs_f64();
        if secs >= 8f64 {
            *last = Instant::now();
            *rsp.body_mut() = Body::wrap_stream(ReaderStream::new(Prometheus::new(secs)));
        } else {
            *rsp.status_mut() = StatusCode::NOT_MODIFIED;
        }
    } else {
        *rsp.status_mut() = StatusCode::PROCESSING;
    }
    Ok(rsp)
}

// 定期发心跳
pub(crate) fn register_target(ctx: &context::Context) {
    if ctx.metrics_url.is_empty() {
        return;
    }
    let url = ctx.metrics_url.to_string();
    let path = "/api/v1/prom/service-discovery/register";
    let url = if url.starts_with("http") {
        format!("{}{}", url, path)
    } else {
        format!("http://{}{}", url, path)
    };
    let port = ctx.port;
    let pool = ctx.service_pool.to_string();
    let local_ip = metrics::local_ip();
    rt::spawn(async move {
        let body = format!(
            r#"
{{
  "labels": {{
    "pool": "{pool}",
    "job": "datamesh-agent"
  }},
  "target": "{local_ip}:{port}"
}}"#
        );
        let client = Client::new();
        let mut interval = interval(Duration::from_secs(60));
        loop {
            let body = body.clone();
            let req = Request::builder()
                .method("PUT")
                .uri(&url)
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .expect("build request");
            let _r = timeout(Duration::from_secs(2), client.request(req)).await;
            log::debug!("target registered: {_r:?}");
            interval.tick().await;
        }
    });
}
