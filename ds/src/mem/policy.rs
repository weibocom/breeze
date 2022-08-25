use std::time::Instant;
// 内存需要缩容时的策略
// 为了避免频繁的缩容，需要设置一个最小频繁，通常使用最小间隔时间
pub struct ShrinkPolicy {
    ticks: usize,
    ticks_interval: u16, // 每隔多少次判断一次
    last: Instant,       // 上一次tick返回true的时间
    secs: u16,           // 每两次tick返回true的最小间隔时间
    id: usize,
}

impl ShrinkPolicy {
    pub fn new() -> Self {
        static ID: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(1);
        let id = ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Self {
            ticks: 0,
            secs: 1800,
            ticks_interval: 1024,
            last: Instant::now(),
            id,
        }
    }
    #[inline]
    pub fn tick(&mut self) -> bool {
        self.ticks += 1;
        if self.ticks < self.ticks_interval as usize {
            return false;
        }
        if self.ticks == self.ticks_interval as usize {
            self.last = Instant::now();
            return false;
        }
        let elapsed = self.last.elapsed().as_secs();
        if elapsed <= self.secs as u64 {
            return false;
        }
        self.ticks = 0;
        true
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
