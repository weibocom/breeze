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
