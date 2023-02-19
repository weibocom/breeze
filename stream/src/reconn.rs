use ds::time::Duration;
use metrics::{Metric, Path};
use std::sync::atomic::{AtomicBool, Ordering::Acquire};
use std::sync::Arc;
pub(crate) struct ReconnPolicy {
    single: Arc<AtomicBool>,
    conns: usize,
    metric: Metric,
    continue_fails: usize,
}

impl ReconnPolicy {
    pub(crate) fn new(path: &Path, single: Arc<AtomicBool>) -> Self {
        Self {
            single,
            metric: path.status("reconn"),
            conns: 0,
            continue_fails: 0,
        }
    }

    // 连接失败，为下一次连接做准备：sleep一段时间，避免无意义的重连
    pub async fn conn_failed(&mut self) {
        self.conns += 1;
        self.metric += 1;
        self.continue_fails += 1;

        static MAX_FAILED: usize = 1;
        // 1. 如果是master，第一次立即重连，后续每隔100ms。
        // 2. 如果是slave，则隔3s重连一次。
        let sleep_ms = if self.single.load(Acquire) {
            if self.continue_fails == 1 {
                0
            } else {
                100
            }
        } else {
            3000
        };

        log::info!("reconn-{}/{} {}", self.conns, self.continue_fails, sleep_ms);
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    }

    // 连接创建成功，连接次数加1，重置持续失败次数
    pub fn connected(&mut self) {
        self.conns += 1;
        self.continue_fails = 0;
    }
}
