#[cfg_attr(any(feature = "enable-log", debug_assertions), path = "enable.rs")]
#[cfg_attr(
    not(any(feature = "enable-log", debug_assertions)),
    path = "disable.rs"
)]
mod init;

pub use init::*;

pub fn log_enabled() -> bool {
    cfg!(feature = "enable-log")
}
