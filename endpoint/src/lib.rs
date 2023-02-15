mod shards;
mod topo;
pub use topo::*;

pub mod cacheservice;
pub mod msgque;
pub mod mysql;
pub mod phantomservice;
pub mod redisservice;

mod refresh;
pub use refresh::{CheckedTopology, RefreshTopology};

// 不同资源默认的超时时间
const TO_PHANTOM_M: Duration = Duration::from_millis(200);
const TO_REDIS_M: Duration = Duration::from_millis(500);
const TO_REDIS_S: Duration = Duration::from_millis(200);
const TO_MC_M: Duration = Duration::from_millis(500);
const TO_MC_S: Duration = Duration::from_millis(80);

trait TimeoutAdjust: Sized {
    fn adjust(&mut self, ms: u32);
    fn to(mut self, ms: u32) -> Self {
        self.adjust(ms);
        self
    }
}

use ds::time::Duration;
impl TimeoutAdjust for Duration {
    fn adjust(&mut self, ms: u32) {
        if ms > 0 {
            *self = Duration::from_millis(ms.max(100) as u64);
        }
    }
}
