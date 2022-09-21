cfg_if::cfg_if! {
if #[cfg(feature = "http")] {

use metrics::prometheus::Prometheus;
use rocket::{
    request::Request,
    response::{stream::ReaderStream, Responder, Response},
    Build, Rocket,
    http::Header,
};

pub(crate) fn init_routes(rocket: Rocket<Build>) -> Rocket<Build> {
    rocket.mount("/metrics", routes![prometheus_metrics])
}

#[get("/")]
fn prometheus_metrics() -> PrometheusMetricsResponse {
    PrometheusMetricsResponse {}
}

pub struct PrometheusMetricsResponse {}

use std::time::{Instant};
use ds::lock::Lock;
use lazy_static::lazy_static;

lazy_static! {
static ref LAST:Lock<Instant> = Instant::now().into();
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
                return response.streamed_body(stream).ok()
            }
            response.header(Header::new("too-frequently", secs.to_string()));
        }
        response.header(Header::new("lock", "failed")).ok()
    }
}



} else {
pub(super) fn init_routes(rocket: Rocket<Build>) -> Rocket<Build> {
    rocket
}
}

}
