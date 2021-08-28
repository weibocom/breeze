use elog::LevelFilter;
use log4rs::{
    append::file::FileAppender,
    config::{Appender, Config, Root},
    encode::pattern::PatternEncoder,
};

use std::io::{Error, ErrorKind, Result};
use std::path::PathBuf;
pub fn init(path: &str, l: &str) -> Result<()> {
    let mut file = PathBuf::new();
    file.push(path);
    file.push("breeze.log");

    let logfile = FileAppender::builder()
        .encoder(Box::new(PatternEncoder::new(
            "[breeze] {d} - {l} - {t} - {m}{n}",
        )))
        .build(file)
        .unwrap();

    let level = match l {
        "trace" | "debug" => LevelFilter::Debug,
        "info" => LevelFilter::Info,
        "warn" => LevelFilter::Warn,
        "error" | "fatal" => LevelFilter::Error,
        _ => LevelFilter::Info,
    };
    let config = Config::builder()
        .appender(Appender::builder().build("logfile", Box::new(logfile)))
        .build(Root::builder().appender("logfile").build(level))
        .unwrap();

    let _handle = log4rs::init_config(config)
        .map_err(|e| Error::new(ErrorKind::InvalidData, format!("init log failed:{:?}", e)))?;

    Ok(())
}
