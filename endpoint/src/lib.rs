mod shards;
mod topo;
pub use topo::*;

pub mod cacheservice;
pub mod kv;
pub mod msgque;
pub mod phantomservice;
pub mod redisservice;
pub mod select;
pub mod uuid;

pub mod dns;

// 不同资源默认的超时时间
const TO_PHANTOM_M: Timeout = Timeout::from_millis(200);
const TO_REDIS_M: Timeout = Timeout::from_millis(500);
const TO_REDIS_S: Timeout = Timeout::from_millis(200);
const TO_MC_M: Timeout = Timeout::from_millis(100); // TODO: 先改成与当前线上实际使用值一致
const TO_MC_S: Timeout = Timeout::from_millis(100); // TODO: 先改成与当前线上实际使用值一致
const TO_MYSQL_M: Timeout = Timeout::from_millis(1000);
const TO_MYSQL_S: Timeout = Timeout::from_millis(500);
const TO_UUID: Timeout = Timeout::from_millis(100);

#[derive(Copy, Clone, Debug)]
pub struct Timeout {
    ms: u16,
}
impl Timeout {
    const fn from_millis(ms: u16) -> Self {
        Self { ms }
    }
    pub fn new(ms: u32) -> Self {
        let mut me = Self { ms: 0 };
        me.adjust(ms);
        me
    }
    pub fn adjust(&mut self, ms: u32) {
        self.ms = ms.max(100).min(6000) as u16;
    }
    pub fn to(mut self, ms: u32) -> Self {
        if ms > 0 {
            self.adjust(ms);
        }

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
