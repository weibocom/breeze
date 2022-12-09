const BUF_MIN: usize = 2 * 1024;
use crate::time::Instant;
// 内存需要缩容时的策略
// 为了避免频繁的缩容，需要设置一个最小频繁，通常使用最小间隔时间
pub struct MemPolicy {
    last: Instant,       // 上一次tick返回true的时间
    check_max: u32,      // 最近一个周期内，最大的内存使用量
    continues: Continue, // 连续多少次tick返回true

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
            check_max: 0,
            continues: Continue(0),
            last: Instant::now(),
            trace: direction.into(),
        }
    }
    #[inline(always)]
    pub fn need_grow(&mut self, len: usize, cap: usize, reserve: usize) -> bool {
        #[cfg(any(feature = "trace", debug_assertions))]
        self.trace.trace_check(len, cap);
        log::debug!("need_grow: len={}, cap={}, reserve={}", len, cap, reserve);
        len + reserve > cap
    }
    #[inline]
    pub fn check_shrink(&mut self, len: usize, _cap: usize) {
        if self.check_max < len as u32 {
            self.check_max = len as u32;
        }
        #[cfg(any(feature = "trace", debug_assertions))]
        self.trace.trace_check(len, _cap);
    }
    // 每个周期（60秒）检查一次，是否满足max * 4 <= cap.
    // 连续10个周期满足条件，则需要缩容
    #[inline]
    pub fn need_shrink(&mut self, len: usize, cap: usize) -> bool {
        log::debug!("need_shrink: len: {}, cap: {} => {}", len, cap, self);
        if cap > BUF_MIN {
            // 每个周期60秒，连续10个周期都满足条件，则需要缩容
            if self.last.elapsed().as_secs() >= 60 {
                if self.check_max < (cap >> 2) as u32 {
                    self.continues.on_tick(self.check_max);
                } else {
                    self.continues.reset();
                    #[cfg(any(feature = "trace", debug_assertions))]
                    self.trace.trace_reset();
                }
                // 重新开始一个周期
                self.last = Instant::now();
                self.check_max = len as u32;
            }
        }
        // 10分钟是个经验值，通常足够让一个在线服务的内存稳定下来
        self.continues.cycles() >= 10
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
        self.continues.reset();
        #[cfg(any(feature = "trace", debug_assertions))]
        self.trace.trace_reset();
        new
    }
    #[inline]
    pub fn shrink(&mut self, len: usize, cap: usize) -> usize {
        let max = self.continues.max() as usize;
        assert!(max < cap, "{}", self);
        let new = (max * 2).max(BUF_MIN).max(len).next_power_of_two();
        log::info!("shrink: {}  < {} => {} {}", len, cap, new, self);
        assert!(new >= len);
        self.continues.reset();

        #[cfg(any(feature = "trace", debug_assertions))]
        self.trace.trace_reset();
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
            "buf policy: last_max:{} max:{} continues:{:?} last: {:?} {:?}",
            self.check_max,
            self.continues.max(),
            self.continues.cycles(),
            self.last.elapsed(),
            self.trace
        )
    }
}

#[cfg(any(feature = "trace", debug_assertions))]
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
#[cfg(not(any(feature = "trace", debug_assertions)))]
mod trace {
    #[derive(Debug)]
    pub(super) struct Trace;
    impl From<&'static str> for Trace {
        fn from(_direction: &'static str) -> Self {
            Self
        }
    }
}

// 1. 高4位表示连续满足shrink条件的次数
// 2. 次28位，表示过去n个满足shrink条件的len的最大值
#[derive(Copy, Clone)]
struct Continue(u32);
const MAX: u32 = 0x0FFF_FFFF;
const MAX_CONTINUES: u32 = 0xF;
impl Continue {
    #[inline]
    fn on_tick(&mut self, last: u32) {
        let cycles = (self.cycles() + 1).min(MAX_CONTINUES);
        let max = self.max().max(last).min(MAX);
        self.0 = (cycles << 28) | max;
    }
    #[inline]
    fn max(&self) -> u32 {
        self.0 & MAX
    }
    #[inline]
    fn cycles(&self) -> u32 {
        self.0 >> 28
    }
    #[inline]
    fn reset(&mut self) {
        self.0 = 0;
    }
}
