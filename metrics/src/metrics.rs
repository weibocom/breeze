use std::fmt::{Display, Formatter};
use std::fmt;

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
    pub(crate) method: CalculateMethod,
    pub(crate) stat_second: u128,
}

impl Metrics {
    pub(crate) fn new(key: String, value: f64, count: usize, method: CalculateMethod, stat_second: u128) -> Metrics {
        Metrics { key, value, count, method, stat_second }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum CalculateMethod {
    Sum = 0,
    Avg = 1,
}

impl Display for CalculateMethod {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            CalculateMethod::Sum => write!(f, "Sum"),
            CalculateMethod::Avg=> write!(f, "Avg"),
        }
    }
}