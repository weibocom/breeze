use elog as log;
pub type Level = log::Level;
pub fn max_level() -> log::LevelFilter {
    log::max_level()
}
#[macro_export]
macro_rules! trace{
    ($($arg:tt)+) => (log::log!(log::Level::Trace, $($arg)+))
}
#[macro_export]
macro_rules! debug{
    ($($arg:tt)+) => (log::log!(log::Level::Debug, $($arg)+))
}
#[macro_export]
macro_rules! info{
    ($($arg:tt)+) => (log::log!(log::Level::Info, $($arg)+))
}

#[macro_export]
macro_rules! warn{
    ($($arg:tt)+) => (log::log!(log::Level::Warn, $($arg)+));
}
#[macro_export]
macro_rules! error{
    ($($arg:tt)+) => (log::log!(log::Level::Error, $($arg)+))
}
#[macro_export]
macro_rules! fatal{
    ($($arg:tt)+) => (log::log!(log::Level::Fatal, $($arg)+))
}
#[macro_export]
macro_rules! log {
    ($lvl:expr, $($arg:tt)+) => ({
        let lvl = $lvl;
        if lvl <= log::max_level() {
            log::private_api_log(
                format_args!($($arg)+),
                lvl,
                &(module_path!(), module_path!(), file!(), line!())
                );
        }
    };)
}
#[inline]
pub fn private_api_log(
    args: std::fmt::Arguments,
    level: Level,
    &(target, module_path, file, line): &(&str, &'static str, &'static str, u32),
) {
    log::__private_api::log(args, level, &(target, module_path, file), line, None);
}
#[macro_export]
macro_rules! log_enabled {
    ($lvl:expr) => {
        $lvl <= log::max_level() && log::private_api_enabled($lvl, module_path!())
    };
}
#[inline]
pub fn private_api_enabled(level: Level, target: &str) -> bool {
    log::__private_api::enabled(level, target)
}

use elog::LevelFilter;
use log4rs::{
    append::rolling_file::{
        policy::compound::{
            roll::fixed_window::FixedWindowRoller, trigger::size::SizeTrigger, CompoundPolicy,
        },
        RollingFileAppender,
    },
    config::{Appender, Config, Root},
    encode::pattern::PatternEncoder,
};

use std::io::{Error, ErrorKind, Result};
use std::path::PathBuf;
pub fn init(path: &str, l: &str) -> Result<()> {
    let mut file = PathBuf::new();
    file.push(path);
    file.push("breeze.log");

    let mut gzfile = PathBuf::new();
    gzfile.push(path);
    gzfile.push("breeze.log{}.gz");

    const MAX_LOG_SIZE: u64 = 1 * 1024 * 1024 * 1024; // 1GB
    const MAX_NUM_LOGS: u32 = 5;
    let policy = Box::new(CompoundPolicy::new(
        Box::new(SizeTrigger::new(MAX_LOG_SIZE)),
        Box::new(
            FixedWindowRoller::builder()
                .base(0)
                .build(
                    gzfile.to_str().ok_or_else(|| {
                        Error::new(ErrorKind::InvalidData, format!("init log failed"))
                    })?,
                    MAX_NUM_LOGS,
                )
                .map_err(|e| {
                    Error::new(ErrorKind::InvalidData, format!("init log failed:{:?}", e))
                })?,
        ),
    ));
    let logfile = RollingFileAppender::builder()
        .encoder(Box::new(PatternEncoder::new(
            "[breeze.{P}] {d} - {l} - {t} - {m}{n}",
        )))
        .build(file, policy)
        .unwrap();

    let level = l.parse().unwrap_or(LevelFilter::Info);
    let config = Config::builder()
        .appender(Appender::builder().build("logfile", Box::new(logfile)))
        .build(Root::builder().appender("logfile").build(level))
        .unwrap();

    let _handle = log4rs::init_config(config)
        .map_err(|e| Error::new(ErrorKind::InvalidData, format!("init log failed:{:?}", e)))?;

    Ok(())
}
