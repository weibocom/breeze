pub use inner::*;
#[cfg(feature = "http")]
mod inner {
    use metrics::prometheus::Prometheus;
    use rocket::{
        http::Header,
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
                return response
                    .header(Header::new("too-frequently", secs.to_string()))
                    .ok();
            }
            response.header(Header::new("lock", "failed")).ok()
        }
    }
}
#[cfg(not(feature = "http"))]
mod inner {
    use rocket::{Build, Rocket};
    pub(crate) fn init_routes(rocket: Rocket<Build>) -> Rocket<Build> {
        rocket
    }
}
