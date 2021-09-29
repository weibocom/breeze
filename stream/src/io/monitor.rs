use std::time::Instant;

use discovery::TopologyTicker;

// 监控一个服务topology的变化. 当发生变化时，需要将client
// attatch到一个新的agent逻辑连接。以便感知变化。
pub(crate) struct Monitor {
    last: usize,
    // 这个值由topology::TopologyWriteGuard负责更新
    ticker: TopologyTicker,
    checked: bool,
    checkpoint: Instant,
    salt: u8, // 一个随机生成的时间
}

impl Monitor {
    pub(crate) fn from(ticker: TopologyTicker) -> Self {
        let salt = rand::random::<usize>() as u8;
        Self {
            checked: false,
            last: ticker.cycle(),
            checkpoint: Instant::now(),
            ticker,
            salt: salt.max(3), // 至少3秒后开始。给后端资源留够初始化的时间。
        }
    }
    // 检查是否需要。满足以下所有条件
    // 1. ticker > last
    // 2. 从满足条件1开始后的超过duration的时间。这个时间是0-255秒的一个随机值。
    #[inline(always)]
    pub(crate) fn check(&mut self) -> bool {
        if self.ticker.cycle() == self.last {
            false
        } else {
            if !self.checked {
                self.checked = true;
                self.checkpoint = Instant::now();
            }
            let ok = self.checkpoint.elapsed().as_secs() >= self.salt as u64;
            if ok {
                self.last = self.ticker.cycle();
                self.checked = false;
            }
            ok
        }
    }
}
