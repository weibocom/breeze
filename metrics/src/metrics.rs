pub(crate) struct MetricsConfig {
    pub(crate) print_only: bool,
    pub(crate) metrics_url: String,
}

impl MetricsConfig {
    pub(crate) fn new (metrics_url: String) -> MetricsConfig {
        MetricsConfig {
            print_only: metrics_url.eq("default"),
            metrics_url,
        }
    }
}
pub(crate) struct Metrics {
    pub(crate) key: String,
    pub(crate) value: f64,
    pub(crate) count: usize,
    pub(crate) stat_second: u128,
}

impl Metrics {
    pub(crate) fn new(key: String, value: f64, count: usize, stat_second: u128) -> Metrics {
        Metrics { key, value, count, stat_second }
    }
}

pub(crate) enum CalculateMethod {
    Sum = 0,
    Avg = 1,
}