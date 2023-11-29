use std::ops::Sub;

pub type Duration = std::time::Duration;

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
#[repr(transparent)]
pub struct Instant(u64);

impl Instant {
    #[inline(always)]
    pub fn now() -> Instant {
        Instant(current_cycle())
    }

    #[inline(always)]
    pub fn elapsed(&self) -> Duration {
        Instant::now() - *self
    }

    #[inline(always)]
    pub fn duration_since(&self, start: Instant) -> Duration {
        let d = match self.0 > start.0 {
            true => self.0 - start.0,
            false => 0,
        };
        Duration::from_nanos(d * nanos_per_cycle() as u64)
    }
}

impl Sub<Instant> for Instant {
    type Output = Duration;

    fn sub(self, other: Instant) -> Duration {
        self.duration_since(other)
    }
}

/// 为方便测试，先写个固定值
#[inline]
fn nanos_per_cycle() -> f64 {
    1.0
}

#[inline]
fn current_cycle() -> u64 {
    #[cfg(target_arch = "x86")]
    use core::arch::x86::_rdtsc;
    #[cfg(target_arch = "x86_64")]
    use core::arch::x86_64::_rdtsc;

    unsafe { _rdtsc() }
}
