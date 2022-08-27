use metrics::{Metric, Path};
use std::time::{Duration, Instant};
pub(crate) struct ReconnPolicy {
    conns: usize,
    metric: Metric,
    last: Instant,
    // 连续出错的次数
    c_err: u8,
}

impl ReconnPolicy {
    pub(crate) fn new(path: &Path) -> Self {
        Self {
            c_err: 0,
            metric: path.status("reconn"),
            conns: 0,
            last: Instant::now(),
        }
    }
    pub fn on_success(&mut self) {
        // 第一次成功不算重试
        if self.conns != 0 {
            self.metric += 1;
        }
        self.c_err = 0;
        self.conns += 1;
        self.last = Instant::now();
    }
    // 如果上一次成功连接超过了1分钟，则sleep 1秒
    pub async fn on_failed(&mut self) {
        self.metric += 1;
        self.c_err = self.c_err.wrapping_add(1);
        self.conns += 1;
        // 前三次连续失败，快速重试。
        let secs = match self.c_err {
            1..=3 => 1,
            _ => 15,
        };
        log::info!("{}-th conn {} sleep:{} secs", self.conns, self.metric, secs);
        tokio::time::sleep(Duration::from_secs(secs)).await;
    }
}
