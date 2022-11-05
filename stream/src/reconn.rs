use metrics::{Metric, Path};
use std::sync::atomic::{AtomicBool, Ordering::Acquire};
use std::sync::Arc;
use std::time::Duration;
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
        let mut sleep_mills = 0;
        if self.continue_fails >= MAX_FAILED {
            // 连续失败超过$MAX_FAILED次，master sleep 100ms，slave sleep 3000
            if self.single.load(Acquire) {
                sleep_mills = 100;
            } else {
                sleep_mills = 3 * 1000;
            }
        };

        if self.continue_fails == 1 {
            log::info!(
                "{}-th conn {} sleep:{} ",
                self.conns,
                self.continue_fails,
                sleep_mills
            );
        } else {
            log::debug!(
                "{}-th conn {} sleep:{} ",
                self.conns,
                self.continue_fails,
                sleep_mills
            );
        }
        tokio::time::sleep(Duration::from_millis(sleep_mills)).await;
    }

    // 连接创建成功，连接次数加1，重置持续失败次数
    pub fn connected(&mut self) {
        self.conns += 1;
        self.continue_fails = 0;
    }
}
