#[cfg_attr(feature = "enable-log", path = "enable.rs")]
#[cfg_attr(not(feature = "enable-log"), path = "disable.rs")]
mod init;

pub use init::*;
