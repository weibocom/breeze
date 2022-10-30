use std::sync::atomic::Ordering;
use std::io::Cursor;

use metrics::prometheus::Prometheus;
use rocket::{
    http::Status,
    request::Request,
    response::{stream::ReaderStream, Responder, Response},
    Build, Rocket,
};

use ds::*;

pub(crate) fn init_routes(rocket: Rocket<Build>) -> Rocket<Build> {
    rocket.mount("/metrics", routes![prometheus_metrics])
        .mount("/memory", routes![memory_stats])
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
  "target": "{}:{}"
}}"#,
            pool,
            metrics::local_ip(),
            port
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

pub struct MemoryStatsResponse {}

#[get("/")]
fn memory_stats() -> MemoryStatsResponse {
    MemoryStatsResponse {}
}

impl<'r> Responder<'r, 'r> for MemoryStatsResponse {
    fn respond_to(self, _: &Request) -> rocket::response::Result<'r> {
        let mut response = Response::build();
        if let Ok(mut last) = LAST.try_lock() {
            let secs = last.elapsed().as_secs_f64();
            if secs >= 1f64 {
                *last = Instant::now();
                let body = format!(
r#"{{
    "allocated": {{
        "size [1 ... 8]": "{}",
        "size [9 ... 16]": "{}",
        "size [17 ... 32]": "{}",
        "size [33 ... 64]": "{}",
        "size [65 ... 128]": "{}",
        "size [129 ... 256]": "{}",
        "size [257 ... 512]": "{}",
        "size [513 ... 1024]": "{}",
        "size [1025 ... 2048]": "{}",
        "size [2049 ... 4096]": "{}",
        "size [4097 ... 8192]": "{}",
        "size [8193 ... 16384]": "{}",
        "size [16385 ... 32768]": "{}",
        "size [32769 ... 65536]": "{}",
        "size [65537 ... 131072]": "{}",
        "size [131073 ... 262144]": "{}",
        "size [262145 ... 524288]": "{}",
        "size [524289 ... 1048576]": "{}",
        "size [1048577 ... 2097152]": "{}",
        "size [2097153 ... 4194304]": "{}",
        "size [4194305 ... 8388608]": "{}",
        "size [8388609 ... 16777216]": "{}",
        "size [16777217 ... 33554432]": "{}",
        "size [33554433 ... 67108864]": "{}",
        "size [67108865 ... ]": "{}",
        "total": "{}",
    }},
    "freed": {{
        "size [1 ... 8]": "{}",
        "size [9 ... 16]": "{}",
        "size [17 ... 32]": "{}",
        "size [33 ... 64]": "{}",
        "size [65 ... 128]": "{}",
        "size [129 ... 256]": "{}",
        "size [257 ... 512]": "{}",
        "size [513 ... 1024]": "{}",
        "size [1025 ... 2048]": "{}",
        "size [2049 ... 4096]": "{}",
        "size [4097 ... 8192]": "{}",
        "size [8193 ... 16384]": "{}",
        "size [16385 ... 32768]": "{}",
        "size [32769 ... 65536]": "{}",
        "size [65537 ... 131072]": "{}",
        "size [131073 ... 262144]": "{}",
        "size [262145 ... 524288]": "{}",
        "size [524289 ... 1048576]": "{}",
        "size [1048577 ... 2097152]": "{}",
        "size [2097153 ... 4194304]": "{}",
        "size [4194305 ... 8388608]": "{}",
        "size [8388609 ... 16777216]": "{}",
        "size [16777217 ... 33554432]": "{}",
        "size [33554433 ... 67108864]": "{}",
        "size [67108865 ... ]": "{}",
        "total": "{}",
    }}
}}"#,
                    ALLOC_SIZE_1_8.load(Ordering::Relaxed),
                    ALLOC_SIZE_9_16.load(Ordering::Relaxed),
                    ALLOC_SIZE_17_32.load(Ordering::Relaxed),
                    ALLOC_SIZE_33_64.load(Ordering::Relaxed),
                    ALLOC_SIZE_65_128.load(Ordering::Relaxed),
                    ALLOC_SIZE_128_256.load(Ordering::Relaxed),
                    ALLOC_SIZE_256_512.load(Ordering::Relaxed),
                    ALLOC_SIZE_512_1K.load(Ordering::Relaxed),
                    ALLOC_SIZE_1K_2K.load(Ordering::Relaxed),
                    ALLOC_SIZE_2K_4K.load(Ordering::Relaxed),
                    ALLOC_SIZE_4K_8K.load(Ordering::Relaxed),
                    ALLOC_SIZE_8K_16K.load(Ordering::Relaxed),
                    ALLOC_SIZE_16K_32K.load(Ordering::Relaxed),
                    ALLOC_SIZE_32K_64K.load(Ordering::Relaxed),
                    ALLOC_SIZE_64K_128K.load(Ordering::Relaxed),
                    ALLOC_SIZE_128K_256K.load(Ordering::Relaxed),
                    ALLOC_SIZE_256K_512K.load(Ordering::Relaxed),
                    ALLOC_SIZE_512K_1M.load(Ordering::Relaxed),
                    ALLOC_SIZE_1M_2M.load(Ordering::Relaxed),
                    ALLOC_SIZE_2M_4M.load(Ordering::Relaxed),
                    ALLOC_SIZE_4M_8M.load(Ordering::Relaxed),
                    ALLOC_SIZE_8M_16M.load(Ordering::Relaxed),
                    ALLOC_SIZE_16M_32M.load(Ordering::Relaxed),
                    ALLOC_SIZE_32M_64M.load(Ordering::Relaxed),
                    ALLOC_SIZE_64M_MORE.load(Ordering::Relaxed),
                    ALLOC_TOTAL.load(Ordering::Relaxed),
                    FREE_SIZE_1_8.load(Ordering::Relaxed),
                    FREE_SIZE_9_16.load(Ordering::Relaxed),
                    FREE_SIZE_17_32.load(Ordering::Relaxed),
                    FREE_SIZE_33_64.load(Ordering::Relaxed),
                    FREE_SIZE_65_128.load(Ordering::Relaxed),
                    FREE_SIZE_128_256.load(Ordering::Relaxed),
                    FREE_SIZE_256_512.load(Ordering::Relaxed),
                    FREE_SIZE_512_1K.load(Ordering::Relaxed),
                    FREE_SIZE_1K_2K.load(Ordering::Relaxed),
                    FREE_SIZE_2K_4K.load(Ordering::Relaxed),
                    FREE_SIZE_4K_8K.load(Ordering::Relaxed),
                    FREE_SIZE_8K_16K.load(Ordering::Relaxed),
                    FREE_SIZE_16K_32K.load(Ordering::Relaxed),
                    FREE_SIZE_32K_64K.load(Ordering::Relaxed),
                    FREE_SIZE_64K_128K.load(Ordering::Relaxed),
                    FREE_SIZE_128K_256K.load(Ordering::Relaxed),
                    FREE_SIZE_256K_512K.load(Ordering::Relaxed),
                    FREE_SIZE_512K_1M.load(Ordering::Relaxed),
                    FREE_SIZE_1M_2M.load(Ordering::Relaxed),
                    FREE_SIZE_2M_4M.load(Ordering::Relaxed),
                    FREE_SIZE_4M_8M.load(Ordering::Relaxed),
                    FREE_SIZE_8M_16M.load(Ordering::Relaxed),
                    FREE_SIZE_16M_32M.load(Ordering::Relaxed),
                    FREE_SIZE_32M_64M.load(Ordering::Relaxed),
                    FREE_SIZE_64M_MORE.load(Ordering::Relaxed),
                    FREE_TOTAL.load(Ordering::Relaxed),
                );
                return response.sized_body(body.len(), Cursor::new(body)).ok();
            }
            return response.status(Status::NotModified).ok();
        }
        response.status(Status::Processing).ok()
    }
}
