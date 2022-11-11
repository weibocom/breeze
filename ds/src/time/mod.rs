#[cfg(feature = "tsc")]
pub type Instant = minstant::Instant;
#[cfg(not(feature = "tsc"))]
pub type Instant = std::time::Instant;

pub type Duration = std::time::Duration;
