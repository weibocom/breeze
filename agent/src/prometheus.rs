use metrics::prometheus::Prometheus;
use rocket::{
    http::Status,
    request::Request,
    response::{stream::ReaderStream, Responder, Response},
    Build, Rocket,
};

pub(crate) fn init_routes(rocket: Rocket<Build>) -> Rocket<Build> {
    rocket.mount("/metrics", routes![prometheus_metrics])
}

#[get("/")]
fn prometheus_metrics() -> PrometheusMetricsResponse {
    PrometheusMetricsResponse {}
}

pub struct PrometheusMetricsResponse {}

use ds::lock::Lock;
use lazy_static::lazy_static;
use std::time::Instant;

lazy_static! {
    static ref LAST: Lock<Instant> = Instant::now().into();
}

impl<'r> Responder<'r, 'r> for PrometheusMetricsResponse {
    fn respond_to(self, _: &Request) -> rocket::response::Result<'r> {
        let mut response = Response::build();
        if let Ok(mut last) = LAST.try_lock() {
            let secs = last.elapsed().as_secs_f64();
            if secs >= 8f64 {
                *last = Instant::now();
                let metrics = Prometheus::new(secs);
                let stream: ReaderStream<Prometheus> = metrics.into();
                return response.streamed_body(stream).ok();
            }
            return response.status(Status::NotModified).ok();
        }
        response.status(Status::Processing).ok()
    }
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
    rt::spawn(async move {
        let body = format!(
            r#"
{{
  "labels": {{
    "pool": "{}",
    "job": "datamesh-agent"
  }},
  "port": {}
}}"#,
            pool, port
        );
        let client = reqwest::Client::new();
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        let mut q = vec![("refresh", true)];
        loop {
            let body = body.clone();
            match client.put(&url).query(&q).body(body).send().await {
                Err(_e) => log::error!("register metrics target failed: {:?} {}", _e, url),
                Ok(r) => {
                    if r.status() != 200 {
                        log::debug!("register metrics target failed: {} {}", url, r.status());
                    }
                    q[0].1 = false
                }
            }
            interval.tick().await;
        }
    });
}
