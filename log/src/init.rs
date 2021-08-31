use elog::LevelFilter;
use log4rs::{append::rolling_file::{RollingFileAppender, policy::compound::{CompoundPolicy, roll::fixed_window::FixedWindowRoller, trigger::size::SizeTrigger}}, config::{Appender, Config, Root}, encode::pattern::PatternEncoder};

use std::io::{Error, ErrorKind, Result};
use std::path::PathBuf;
pub fn init(path: &str) -> Result<()> {
    let mut file = PathBuf::new();
    file.push(path);
    file.push("breeze.log");

    let mut gzfile = PathBuf::new();
    gzfile.push(path);
    gzfile.push("breeze.log.{}.gz");

    const MAX_LOG_SIZE: u64 = 1 * 1024 * 1024 * 1024 ; // 1GB
    const MAX_NUM_LOGS: u32 = 5;
    let policy = Box::new(CompoundPolicy::new(
        Box::new(SizeTrigger::new(MAX_LOG_SIZE)),
        Box::new(
            FixedWindowRoller::builder()
                .base(0)
                .build(
                    gzfile.to_str()
                    .ok_or_else(||Error::new(ErrorKind::InvalidData, format!("init log failed")))?,
                    MAX_NUM_LOGS,
                )
                .map_err(|e| Error::new(ErrorKind::InvalidData, format!("init log failed:{:?}", e)))?,
        ),
    ));
    let logfile = RollingFileAppender::builder()
        .encoder(Box::new(PatternEncoder::new(
            "[breeze] {d} - {l} - {t} - {m}{n}",
        )))
        .build(file, policy)
        .unwrap();

    let level = if cfg!(debug_assertions) {
        LevelFilter::Debug
    } else {
        LevelFilter::Info
    };
    let config = Config::builder()
        .appender(Appender::builder().build("logfile", Box::new(logfile)))
        .build(Root::builder().appender("logfile").build(level))
        .unwrap();

    let _handle = log4rs::init_config(config)
        .map_err(|e| Error::new(ErrorKind::InvalidData, format!("init log failed:{:?}", e)))?;

    Ok(())
}
//use std::io::Result;
//pub fn init(_path: &str) -> Result<()> {
//    Ok(())
//}
