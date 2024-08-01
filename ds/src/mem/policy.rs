const BUF_MIN: usize = 2 * 1024;
// 内存需要缩容时的策略
// 为了避免频繁的缩容，需要设置一个最小频繁，通常使用最小间隔时间
#[derive(Debug)]
pub struct MemPolicy {
    max: u32,    // 最近一个周期内，最大的内存使用量
    cycles: u32, // 连续多少次tick返回true

    // 下面两个变量为了输出日志
    trace: trace::Trace,
}

impl MemPolicy {
    pub fn tx() -> Self {
        Self::with_direction("tx")
    }
    pub fn rx(_min: usize, _max: usize) -> Self {
        Self::with_direction("rx")
    }
    pub fn with_direction(direction: &'static str) -> Self {
        Self::from(direction)
    }
    fn from(direction: &'static str) -> Self {
        Self {
            max: 0,
            cycles: 0,
            trace: direction.into(),
        }
    }
    #[inline(always)]
    pub fn need_grow(&mut self, len: usize, cap: usize, reserve: usize) -> bool {
        #[cfg(any(feature = "trace"))]
        self.trace.trace_check(len, cap);
        log::debug!("need_grow: len={}, cap={}, reserve={}", len, cap, reserve);
        len + reserve > cap
    }
    #[inline]
    pub fn check_shrink(&mut self, len: usize, _cap: usize) {
        if self.max < len as u32 {
            self.max = len as u32;
        }
        #[cfg(any(feature = "trace"))]
        self.trace.trace_check(len, _cap);
    }
    // 调用方定期调用need_shrink，如果返回true，则需要缩容
    // 1. 如果max > 1/4 cap，则重置max.
    // 2. 如果连续20个周期满足 max < 1/4 cap，则返回true
    #[inline]
    pub fn need_shrink(&mut self, len: usize, cap: usize) -> bool {
        log::debug!("need_shrink: len: {}, cap: {} => {}", len, cap, self);
        if cap > BUF_MIN {
            self.check_shrink(len, cap);
            // 每10个周期检查一次。如果max不满足缩容要求，则重置
            if self.max as usize * 4 > cap {
                self.reset();
                return false;
            }
            // 满足缩容条件
            self.cycles += 1;
        }
        // 1. 连续20个周期满足缩容条件, 并且len为0，这样避免缩容时的数据拷贝。
        // 2. 连续64个周期满足缩容条件，这样避免频繁的缩容。
        (self.cycles >= 20 && len == 0) || self.cycles >= 64
    }
    #[inline]
    fn reset(&mut self) {
        self.max = 0;
        self.cycles = 0;
        #[cfg(any(feature = "trace"))]
        self.trace.trace_reset();
    }
    // 确认缩容的size
    // 1. 最小值为 len + reserve的1.25倍
    // 2. 不小于原来的cap
    // 3. 至少为BUF_MIN
    // 4. 2的指数倍
    #[inline]
    pub fn grow(&mut self, len: usize, cap: usize, reserve: usize) -> usize {
        let new = ((5 * (len + reserve)) / 4)
            .max(cap)
            .max(BUF_MIN)
            .next_power_of_two();
        if cap > BUF_MIN {
            log::info!("grow: {}+{}>{} => {} {}", len, reserve, cap, new, self);
        }
        self.reset();
        new
    }
    #[inline]
    pub fn shrink(&mut self, len: usize, cap: usize) -> usize {
        let max = self.max as usize;
        assert!(max < cap, "{}", self);
        let new = (max * 2).max(BUF_MIN).max(len).next_power_of_two();
        log::info!("shrink: {}  < {} => {} {}", len, cap, new, self);
        assert!(new >= len);
        self.reset();
        new
    }
}

impl Drop for MemPolicy {
    fn drop(&mut self) {
        log::info!("buf policy drop => {}", self);
    }
}
use std::fmt::{self, Display, Formatter};
impl Display for MemPolicy {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "buf policy:  max:{} cycles:{} {:?}",
            self.max, self.cycles, self.trace
        )
    }
}

#[cfg(any(feature = "trace"))]
mod trace {
    use crate::time::Instant;
    use std::fmt::{self, Debug, Formatter};
    pub(super) struct Trace {
        direction: &'static str, // 方向: true为tx, false为rx. 打日志用
        id: usize,
        start: Instant,
        max: usize, // 上一个周期内，最大的len
        checks: usize,
        last_checks: usize,
        cap: usize,
    }

    impl From<&'static str> for Trace {
        fn from(direction: &'static str) -> Self {
            static ID: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(1);
            let id = ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Self {
                direction,
                id,
                start: Instant::now(),
                max: 0,
                checks: 0,
                cap: 0,
                last_checks: 0,
            }
        }
    }
    impl Debug for Trace {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(
                f,
                " id: {} trace max:{} total checks:{} last checks:{} cap:{}, lifetime:{:?} => {}",
                self.id,
                self.max,
                self.checks,
                self.last_checks,
                self.cap,
                self.start.elapsed(),
                self.direction
            )
        }
    }
    impl Trace {
        #[inline]
        pub(super) fn trace_check(&mut self, len: usize, cap: usize) {
            self.checks += 1;
            self.last_checks += 1;
            if self.cap != cap {
                self.cap = cap;
            }
            self.max = self.max.max(len);
        }
        #[inline]
        pub(super) fn trace_reset(&mut self) {
            self.max = 0;
            self.last_checks = 0;
        }
    }
}
#[cfg(not(any(feature = "trace")))]
mod trace {
    #[derive(Debug)]
    pub(super) struct Trace;
    impl From<&'static str> for Trace {
        fn from(_direction: &'static str) -> Self {
            Self
        }
    }
}
