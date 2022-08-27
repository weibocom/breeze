const BUF_MIN: usize = 1024;
use std::time::{Duration, Instant};
// 内存需要缩容时的策略
// 为了避免频繁的缩容，需要设置一个最小频繁，通常使用最小间隔时间
pub struct MemPolicy {
    ticks: usize,
    last: Instant, // 上一次tick返回true的时间
    secs: u16,     // 每两次tick返回true的最小间隔时间

    // 下面两个变量为了输出日志
    direction: &'static str, // 方向: true为tx, false为rx. 打日志用
    id: usize,
    start: Instant,
}

impl MemPolicy {
    pub fn tx() -> Self {
        Self::with_direction("tx")
    }
    pub fn rx() -> Self {
        Self::with_direction("rx")
    }
    pub fn with_direction(direction: &'static str) -> Self {
        Self::from(Duration::from_secs(1800), direction)
    }
    fn from(delay: Duration, direction: &'static str) -> Self {
        static ID: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(1);
        let id = ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let secs = delay.as_secs().max(1).min(u16::MAX as u64) as u16;
        Self {
            ticks: 0,
            secs,
            last: Instant::now(),
            id,
            direction,
            start: Instant::now(),
        }
    }
    #[inline(always)]
    pub fn need_grow(&self, len: usize, cap: usize, reserve: usize) -> bool {
        len + reserve > cap
    }
    // 每隔31次进行一次check
    // 连续self.secs秒check返回true，则需要缩容
    #[inline]
    pub fn need_shrink(&mut self, len: usize, cap: usize) -> bool {
        if len * 4 < cap || cap <= BUF_MIN {
            self.reset();
            return false;
        }
        self.ticks += 1;
        // 定期检查。
        const TICKS: usize = 31;
        if self.ticks & TICKS != 0 {
            return false;
        }
        if self.ticks == TICKS + 1 {
            self.last = Instant::now();
            return false;
        }
        if self.last.elapsed().as_secs() <= self.secs as u64 {
            return false;
        }
        true
    }
    #[inline(always)]
    fn reset(&mut self) {
        if self.ticks > 0 {
            self.ticks = 0;
        }
    }
    // 确认缩容的size
    // 1. 最小值为 len + reserve的1.25倍
    // 2. 不小于原来的cap
    // 3. 至少为BUF_MIN
    // 4. 2的指数倍
    #[inline]
    pub fn grow(&self, len: usize, cap: usize, reserve: usize) -> usize {
        let new = ((5 * (len + reserve)) / 4)
            .max(cap)
            .max(BUF_MIN)
            .next_power_of_two();
        log::info!(
            "{} buf grow: {} {} + {} => {} id:{}",
            self.direction,
            len,
            cap,
            reserve,
            new,
            self.id
        );
        new
    }
    // 确认缩容的size:
    // 1. 当前容量的一半
    // 2. 最小值为MIN_BUF
    // 3. 最小值为len
    // 4. 取2的指数倍
    // 注意：返回值可能比输入的cap大. 但在判断need_shrink时，会判断len * 4 < cap, 所以不会出现len * 4 < cap, 但是cap / 2 < len的情况
    #[inline]
    pub fn shrink(&mut self, len: usize, cap: usize) -> usize {
        let new = (cap / 2).max(BUF_MIN).max(len).next_power_of_two();
        self.ticks = 0;
        log::info!(
            "{} buf shrink: {} {} => {} ticks:{} elapse:{} secs id:{}",
            self.direction,
            len,
            cap,
            new,
            self.ticks,
            self.last.elapsed().as_secs(),
            self.id
        );
        new
    }
}

impl Drop for MemPolicy {
    fn drop(&mut self) {
        log::info!(
            "{} buf policy drop. lifetime:{:?} id:{}",
            self.direction,
            self.start.elapsed(),
            self.id
        );
    }
}
