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
macro_rules! _warn{
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
                &(module_path!(), module_path!(), file!(), line!()),
                None,
                );
        }
    };)
}
pub use {_warn as warn, debug, error, fatal, info, trace};
#[inline]
pub fn private_api_log(
    args: std::fmt::Arguments,
    level: Level,
    &(target, module_path, file, line): &(&str, &'static str, &'static str, u32),
    kvs: Option<&[(&str, &dyn log::kv::ToValue)]>,
) {
    log::__private_api_log(args, level, &(target, module_path, file, line), kvs);
}
#[macro_export]
macro_rules! log_enabled {
    ($lvl:expr) => {
        $lvl <= log::max_level() && log::private_api_enabled($lvl, module_path!())
    };
}
#[inline]
pub fn private_api_enabled(level: Level, target: &str) -> bool {
    log::__private_api_enabled(level, target)
}
