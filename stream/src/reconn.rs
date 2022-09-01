use metrics::{Metric, Path};
use std::sync::atomic::{AtomicBool, Ordering::Acquire};
use std::sync::Arc;
use std::time::Duration;
pub(crate) struct ReconnPolicy {
    single: Arc<AtomicBool>,
    conns: usize,
    metric: Metric,
}

impl ReconnPolicy {
    pub(crate) fn new(path: &Path, single: Arc<AtomicBool>) -> Self {
        Self {
            single,
            metric: path.status("reconn"),
            conns: 0,
        }
    }
    // 第一次，不处理。
    pub async fn check(&mut self) {
        self.conns += 1;
        if self.conns == 1 {
            return;
        }
        self.metric += 1;
        let sleep = if self.single.load(Acquire) {
            Duration::from_millis(100)
        } else {
            Duration::from_millis(3 * 1000)
        };
        log::info!("{}-th conn {} sleep:{:?} ", self.conns, self.metric, sleep);
        tokio::time::sleep(sleep).await;
    }
}
