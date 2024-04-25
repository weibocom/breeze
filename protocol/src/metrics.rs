use std::fmt::{self, Display, Formatter};

use metrics::{Metric, Path};

/// 用于统计ip维度的req-rtt，get、store、其他的qps，目前只用于msgque fishermen
pub struct HostMetric {
    pub get: Metric,
    pub store: Metric,
    pub others: Metric,
}

impl From<Path> for HostMetric {
    fn from(path: Path) -> Self {
        HostMetric {
            get: path.qps("get"),
            store: path.qps("store"),
            others: path.qps("others"),
        }
    }
}

impl Display for HostMetric {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "get: {}, store: {}, others: {}",
            self.get, self.store, self.others
        )
    }
}
