mod shards;
mod topo;
pub use topo::*;

pub mod cacheservice;
pub mod msgque;
pub mod phantomservice;
pub mod redisservice;

mod refresh;
pub use refresh::{CheckedTopology, RefreshTopology};

mod notify;
pub use notify::Notify;

// 不同资源默认的超时时间
const TO_PHANTOM_M: Timeout = Timeout::from_millis(200);
const TO_REDIS_M: Timeout = Timeout::from_millis(500);
const TO_REDIS_S: Timeout = Timeout::from_millis(50);
const TO_MC_M: Timeout = Timeout::from_millis(500);
const TO_MC_S: Timeout = Timeout::from_millis(50);

#[derive(Copy, Clone, Debug)]
pub struct Timeout {
    ms: u16,
}
impl Timeout {
    const fn from_millis(ms: u16) -> Self {
        Self { ms }
    }
    pub fn adjust(&mut self, ms: u32) {
        self.ms = ms.max(50).min(6000) as u16;
    }
    pub fn to(mut self, ms: u32) -> Self {
        self.adjust(ms);
        self
    }
    pub fn ms(&self) -> u16 {
        self.ms
    }
}

use std::time::Duration;
impl Into<Duration> for Timeout {
    fn into(self) -> Duration {
        Duration::from_millis(self.ms as u64)
    }
}
