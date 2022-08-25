use std::time::{Duration, Instant};
// 内存需要缩容时的策略
// 为了避免频繁的缩容，需要设置一个最小频繁，通常使用最小间隔时间
pub struct ShrinkPolicy {
    ticks: usize,
    last: Instant, // 上一次tick返回true的时间
    secs: u32,     // 每两次tick返回true的最小间隔时间
    id: usize,
}

impl ShrinkPolicy {
    pub fn new() -> Self {
        Self::from(Duration::from_secs(600))
    }
    pub fn from(delay: Duration) -> Self {
        static ID: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(1);
        let id = ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let secs = delay.as_secs().max(1).min(86400) as u32;
        Self {
            ticks: 0,
            secs,
            last: Instant::now(),
            id,
        }
    }
    #[inline]
    pub fn tick(&mut self) -> bool {
        self.ticks += 1;
        // 定期检查。
        const TICKS: usize = 31;
        if self.ticks & TICKS == 0 {
            if self.ticks == TICKS + 1 {
                self.last = Instant::now();
                return false;
            }
            if self.last.elapsed().as_secs() <= self.secs as u64 {
                return false;
            }
            self.ticks = 0;
            true
        } else {
            false
        }
    }
    #[inline]
    pub fn reset(&mut self) {
        if self.ticks > 0 {
            self.ticks = 0;
        }
    }
    #[inline]
    pub fn id(&self) -> usize {
        self.id
    }
}
