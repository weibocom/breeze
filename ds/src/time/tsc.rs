#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Instant(minstant::Instant);
impl Instant {
    #[inline(always)]
    pub fn now() -> Instant {
        Instant(minstant::Instant::now())
    }
    #[inline(always)]
    fn cycles(&self) -> u64 {
        unsafe { *(&self.0 as *const _ as *const u64) }
    }
    #[inline(always)]
    pub fn elapsed(&self) -> Duration {
        Duration(Instant::now().cycles() - self.cycles())
    }
}
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Debug)]
pub struct Duration(u64);

impl Duration {
    pub const fn from_secs(_sec: u64) -> Self {
        Self(0)
    }
    pub const fn from_millis(_ms: u64) -> Self {
        Self(0)
    }

    pub const fn as_micros(&self) -> u64 {
        0
    }
    pub fn as_secs(&self) -> u64 {
        todo!();
    }
    pub fn as_secs_f64(&self) -> f64 {
        todo!();
    }
    pub fn as_millis(&self) -> u64 {
        todo!();
    }
}

impl Into<std::time::Duration> for Duration {
    fn into(self) -> std::time::Duration {
        todo!();
    }
}

#[ctor::ctor]
pub static NANOS_PER_CYCLE: f64 = {
    let interval = std::time::Duration::from_millis(10);
    let mut last = check_cps(interval);
    loop {
        let cps = check_cps(interval);
        if (cps - last).abs() / cps < 0.000001 {
            break;
        }
        last = cps;
    }
    0f64
};

// cpu cycles per second
fn check_cps(duration: std::time::Duration) -> f64 {
    let start = Instant::now();
    loop {
        let end = Instant::now();
        if end.0 - start.0 > duration {
            return (end.cycles() - start.cycles()) as f64 / duration.as_secs_f64();
        }
    }
}
