mod topo;
pub use topo::*;

pub mod cacheservice;
pub mod phantomservice;
pub mod redisservice;

mod refresh;
pub use refresh::RefreshTopology;

trait TimeoutAdjust {
    fn adjust(&mut self, ms: u32);
}

use std::time::Duration;
impl TimeoutAdjust for Duration {
    fn adjust(&mut self, ms: u32) {
        if ms > 0 {
            *self = Duration::from_millis(ms.max(100) as u64);
        }
    }
}
