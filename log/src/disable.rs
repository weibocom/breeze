#[macro_export]
macro_rules! noop{
    ($($arg:tt)+) => {
        {
            let _ = format_args!($($arg)+);
            ()
        }
    };
}
#[macro_export]
macro_rules! trace {
    ($($arg:tt)+) => {
        log::noop!($($arg)+)
    };
}
#[macro_export]
macro_rules! debug {
    ($($arg:tt)+) => {
        log::noop!($($arg)+)
    };
}
#[macro_export]
macro_rules! info {
    ($($arg:tt)+) => {
        log::noop!($($arg)+)
    };
}

#[macro_export]
macro_rules! _warn {
    ($($arg:tt)+) => {
        log::noop!($($arg)+)
    };
}
#[macro_export]
macro_rules! error {
    ($($arg:tt)+) => {
        log::noop!($($arg)+)
    };
}
#[macro_export]
macro_rules! fatal {
    ($($arg:tt)+) => {
        log::noop!($($arg)+)
    };
}
#[macro_export]
macro_rules! log_enabled {
    ($lvl:expr) => {
        false
    };
}
pub use {_warn as warn, debug, error, fatal, info, trace};

use std::io::Write;
pub fn init(path: &str, _l: &str) -> std::io::Result<()> {
    std::fs::create_dir_all(path)?;
    let mut log = std::fs::File::create(format!("{}/breeze.log", path))?;
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let hint = format!("===> log disabled: {} secs <===", secs);
    log.write(hint.as_bytes())?;
    Ok(())
}
