use ds::time::{sleep, Duration};
pub(crate) struct ReconnPolicy {
    conns: usize,
    continue_fails: usize,
}

impl ReconnPolicy {
    pub(crate) fn new() -> Self {
        Self {
            conns: 0,
            continue_fails: 0,
        }
    }

    // 连接失败，为下一次连接做准备：sleep一段时间，避免无意义的重连
    // 第一次快速重连
    pub async fn conn_failed(&mut self) {
        self.conns += 1;

        // 第一次失败的时候，continue_fails为0，因此不会sleep
        let sleep_mills = (self.continue_fails * 500).min(6000);
        log::info!(
            "{}-th conn {} sleep:{}ms",
            self.conns,
            self.continue_fails,
            sleep_mills,
        );
        self.continue_fails += 1;
        if sleep_mills > 0 {
            sleep(Duration::from_millis(sleep_mills as u64)).await;
        }
    }

    // 连接创建成功，连接次数加1，重置持续失败次数
    pub fn connected(&mut self) {
        self.conns += 1;
        self.continue_fails = 0;
    }
}
