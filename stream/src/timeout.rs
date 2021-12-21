use std::time::{Duration, Instant};

use protocol::{Error, Result};
// 在duration的时间内，请求数量必须大于等于lease，否则认定为超时。
pub(crate) struct TimeoutChecker {
    checkpoint: Instant,
    cycle: Duration,
    least: u32,
    ticks: usize,
}

impl TimeoutChecker {
    #[inline(always)]
    pub(crate) fn new(cycle: Duration, least: u32) -> Self {
        Self {
            cycle,
            least,
            checkpoint: Instant::now(),
            ticks: 0,
        }
    }
    #[inline(always)]
    pub(crate) fn tick(&mut self) {
        self.ticks += 1;
    }
    #[inline(always)]
    pub(crate) fn reset(&mut self) {
        self.ticks = 0; // 重新开始
        self.checkpoint = Instant::now();
    }

    #[inline(always)]
    pub(crate) fn check(&mut self) -> Result<()> {
        if self.ticks >= self.least as usize {
            self.reset();
            return Ok(());
        }
        // 还未到检查点
        if self.checkpoint.elapsed() < self.cycle {
            return Ok(());
        }
        Err(Error::Timeout((self.checkpoint.elapsed(), self.least)))
    }
}
