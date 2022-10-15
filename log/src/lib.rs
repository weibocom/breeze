#[cfg_attr(any(feature = "enable-log", debug_assertions), path = "enable.rs")]
#[cfg_attr(not(any(feature = "enable-log", debug_assertions)), path = "enable.rs")]
mod init;

pub use init::*;
