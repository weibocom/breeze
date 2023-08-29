#[cfg(feature = "tsc")]
pub type Instant = minstant::Instant;
pub type Anchor = minstant::Anchor;
#[cfg(not(feature = "tsc"))]
pub type Instant = std::time::Instant;

pub type Duration = std::time::Duration;
