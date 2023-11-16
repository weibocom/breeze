#[cfg(feature = "tsc")]
mod tsc;
#[cfg(not(feature = "tsc"))]
mod tsc {
    pub type Instant = minstant::Instant;
    pub type Duration = std::time::Duration;
}

pub use tsc::*;

mod tokio;
pub use self::tokio::*;

// TSC时钟源是否稳定可用
pub fn tsc_stable() -> bool {
    std::fs::read_to_string("/sys/devices/system/clocksource/clocksource0/available_clocksource")
        .map(|s| s.contains("tsc"))
        .unwrap_or(false)
}
