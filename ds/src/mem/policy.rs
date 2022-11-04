const BUF_MIN: usize = 2 * 1024;
use std::time::Instant;
// 内存需要缩容时的策略
// 为了避免频繁的缩容，需要设置一个最小频繁，通常使用最小间隔时间
pub struct MemPolicy {
    last: Instant, // 上一次tick返回true的时间
    max: usize,    // 最近一个周期内，最大的内存使用量。

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
            last: Instant::now(),
            trace: direction.into(),
        }
    }
    #[inline(always)]
    pub fn need_grow(&self, len: usize, cap: usize, reserve: usize) -> bool {
        log::debug!("need_grow: len={}, cap={}, reserve={}", len, cap, reserve);
        len + reserve > cap
    }
    #[inline]
    pub fn check_shrink(&mut self, len: usize, _cap: usize) {
        if self.max < len {
            self.reset(len);
        }
    }
    fn reset(&mut self, len: usize) {
        self.max = len;
        self.last = Instant::now();
    }
    // 在一个周期内，max * 4 <= cap. 则需要缩容
    #[inline]
    pub fn need_shrink(&mut self, len: usize, cap: usize) -> bool {
        log::debug!("need_shrink: len: {}, cap: {} => {}", len, cap, self);
        if cap > BUF_MIN {
            self.check_shrink(len, cap);
            // 600秒对于大部分在线业务，足够得出稳定的max值。
            if self.last.elapsed().as_secs() >= 600 {
                if self.max < (cap >> 2) {
                    // 只有当前buff size是0时才触发缩容。
                    return true;
                }
                // 重新开始一个周期
                self.reset(len);
            }
        } else {
            self.reset(len);
        }
        false
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
        log::info!("grow: {} {} > {} => {} {}", len, reserve, cap, new, self);
        self.max = 0;
        new
    }
    #[inline]
    pub fn shrink(&mut self, len: usize, cap: usize) -> usize {
        assert!(self.max < cap, "{}", self);
        let new = (self.max * 2).max(BUF_MIN).max(len).next_power_of_two();
        log::info!("shrink: {}  < {} => {} {}", len, cap, new, self);
        assert!(new >= len);
        self.max = 0;
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
            "buf policy: max: {} last: {:?} {:?}",
            self.max,
            self.last.elapsed(),
            self.trace
        )
    }
}

#[cfg(debug_assertions)]
mod trace {
    use std::fmt::{self, Debug, Formatter};
    use std::time::Instant;
    pub(super) struct Trace {
        direction: &'static str, // 方向: true为tx, false为rx. 打日志用
        id: usize,
        start: Instant,
    }

    impl From<&'static str> for Trace {
        fn from(direction: &'static str) -> Self {
            static ID: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(1);
            let id = ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Self {
                direction,
                id,
                start: Instant::now(),
            }
        }
    }
    impl Debug for Trace {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(
                f,
                " id: {} lifetime:{:?} => {}",
                self.id,
                self.start.elapsed(),
                self.direction
            )
        }
    }
}
#[cfg(not(debug_assertions))]
mod trace {
    #[derive(Debug)]
    pub(super) struct Trace;
    impl From<&'static str> for Trace {
        fn from(_direction: &'static str) -> Self {
            Self
        }
    }
}
