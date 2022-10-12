extern crate lazy_static;
use clap::{FromArgMatches, IntoApp, Parser};
use lazy_static::lazy_static;
use std::path::Path;
use std::{
    io::{Error, ErrorKind, Result},
    vec,
};
use url::Url;

mod quadruple;
pub use quadruple::Quadruple;

#[derive(Parser, Debug)]
#[clap(name = "breeze", version = "0.0.1", author = "IF")]
pub struct ContextOption {
    #[clap(long, help("port for suvervisor"), default_value("9984"))]
    pub port: u16,

    #[clap(short, long, help("number of threads"), default_value("4"))]
    pub thread_num: u8,

    #[clap(long, help("number of open file"), default_value("204800"))]
    pub no_file: u64,

    #[clap(
        short,
        long,
        help("service registry url. e.g. vintage://127.0.0.1:8080"),
        default_value("vintage://127.0.0.1:8080")
    )]
    pub discovery: Url,

    #[clap(
        short,
        long,
        help("idc config path"),
        default_value("/3/config/breeze/idc_region")
    )]
    pub idc_path: String,

    #[clap(
        long,
        help("interval of updating config (unit second)"),
        default_value("15")
    )]
    tick_sec: usize,

    #[clap(
        short,
        long("snapshot"),
        help("path for saving snapshot of service topology."),
        default_value("/tmp/breeze/snapshot")
    )]
    pub snapshot_path: String,
    #[clap(
        short('p'),
        long,
        help("path for unix domain socket to listen."),
        default_value("/tmp/breeze/socks")
    )]
    pub service_path: String,

    #[clap(short, long, help("starting in upgrade mode"))]
    upgrade: bool,

    #[clap(short, long, help("log path"), default_value("/tmp/breeze/logs"))]
    pub log_dir: String,

    #[clap(short, long, help("metrics url"))]
    pub metrics_url: Option<String>,

    #[clap(
        long,
        help("establish a connection to select an local ip"),
        default_value("10.10.10.10:53")
    )]
    pub metrics_probe: String,

    #[clap(long, help("log level. debug|info|warn|error"), default_value("info"))]
    pub log_level: String,

    #[clap(long, help("service pool"), default_value("default_pool"))]
    pub service_pool: String,

    #[clap(long, help("cpu level"), default_value("vx"))]
    pub cpu: String,

    // api参数，目前只有这一个差异参数，先放这里
    #[clap(long, help("api whitelist host"), default_value("localhost"))]
    pub whitelist_host: String,
}

lazy_static! {
    // rmlog-760-g3a7a12b-modified
    // 取 g3a7a12b-modified
    static ref SHORT_VERSION: String = {
        let full = git_version::git_version!();
        let fields:Vec<&str> = full.split('-').collect();
        let len = fields.len();
        let last = *fields.get(len-1).unwrap_or(&"");
        if last == "modified" {
            let second_last = fields.get(len-2).unwrap_or(&"");
            format!("{}_{}", second_last, last)
        } else {
            last.to_string()
        }
    };
    static ref CONTEXT: Context = {
        let ctx = ContextOption::from_os_args();
        ctx.check().expect("context check failed");
        let ctx = Context::from(ctx);
        ctx
    };
}
impl ContextOption {
    #[inline]
    pub fn from_os_args() -> Self {
        let app = <Self as IntoApp>::command().version(&SHORT_VERSION[..]);
        let matches = app.get_matches();
        <Self as FromArgMatches>::from_arg_matches(&matches).expect("parse args failed")
    }
    // service_path目录要存在
    pub fn check(&self) -> Result<()> {
        let path = Path::new(&self.service_path);
        if !path.is_dir() {
            let msg = format!("{} is not a valid dir", self.service_path);
            return Err(Error::new(ErrorKind::NotFound, msg));
        }
        if self.tick_sec < 1 || self.tick_sec > 60 {
            return Err(Error::new(ErrorKind::InvalidData, "tick must be in [1,60]"));
        }
        Ok(())
    }

    pub fn tick(&self) -> std::time::Duration {
        assert!(self.tick_sec >= 1 && self.tick_sec <= 60);
        std::time::Duration::from_secs(self.tick_sec as u64)
    }
    // 如果是以升级模式启动，则会将原有的端口先关闭。
    pub fn listeners(&self) -> ListenerIter {
        ListenerIter {
            path: self.service_path.to_string(),
            processed: Default::default(),
        }
    }
}

use std::collections::HashMap;
pub struct ListenerIter {
    processed: HashMap<String, String>,
    path: String,
}

impl ListenerIter {
    pub fn from(path: String) -> Self {
        Self {
            processed: Default::default(),
            path,
        }
    }

    // 扫描self.pah，获取该目录下所有不以.sock结尾，符合格式的文件作为服务配置进行解析。
    // 不以.sock结尾，由'@'字符分隔成一个Quard的配置。一个标准的服务配置文件名为
    // 如果对应的文件已经存在 $name.sock。那说明有其他进程侦听了该服务，如果协议或端口不同则说明冲突；
    // unix的配置放在前面
    // 返回解析成功的Quadruple和解析失败的数量
    pub async fn scan(&mut self) -> (Vec<Quadruple>, usize) {
        let mut failed = 0;
        let mut listeners = vec![];
        match self.read_all().await {
            Ok(names) => {
                for name in names {
                    if let Some(one) = Quadruple::parse(&self.path, &name) {
                        if !self.processed.contains_key(one.service()) {
                            listeners.push(one);
                        } else {
                            // 包含了service，但端口、协议等任何其他发生变化，则立即汇报
                            let empty = "default-empty".to_string();
                            let old = self.processed.get(one.service()).unwrap_or(&empty);
                            if old.ne(&one.name()) {
                                log::warn!(
                                    "sock scan found conflict, old:{}, new:{}",
                                    old,
                                    one.name()
                                );
                                failed += 1;
                            }
                        }
                    }
                }
            }
            Err(_e) => {
                log::warn!("failed to scan '{}' err:{:?}", self.path, _e);
                failed += 1;
            }
        }
        if listeners.len() > 0 {
            // 排序。同时注册了tcp与unix则优先使用unix
            listeners.sort();
            listeners.retain(|item| {
                let retain = !self.processed.contains_key(item.service());
                self.processed
                    .insert(item.service().to_string(), item.name());
                if !retain {
                    log::warn!("{} register in multiple family", item.service());
                }
                retain
            });
        }
        (listeners, failed)
    }

    pub async fn remove_unix_sock(&mut self) -> Result<()> {
        let mut dir = tokio::fs::read_dir(&self.path).await?;
        while let Some(child) = dir.next_entry().await? {
            let path = child.path();
            let is_unix_sock = path.to_str().map(|s| s.ends_with(".sock")).unwrap_or(false);
            if is_unix_sock {
                log::info!("{:?} exists. deleting", path);
                let _ = tokio::fs::remove_file(path).await;
            }
        }
        Ok(())
    }
    async fn read_all(&self) -> Result<Vec<String>> {
        let mut found = vec![];
        let mut dir = tokio::fs::read_dir(&self.path).await?;
        while let Some(child) = dir.next_entry().await? {
            if child.metadata().await?.is_file() {
                match child.path().into_os_string().into_string() {
                    Ok(name) => {
                        found.push(name);
                    }
                    Err(_os_str) => log::warn!("{:?} is not a valid file name", _os_str),
                }
            }
        }
        Ok(found)
    }
    pub async fn files(&self) -> Result<Vec<String>> {
        let mut found = vec![];
        let mut dir = tokio::fs::read_dir(&self.path).await?;
        while let Some(child) = dir.next_entry().await? {
            if child.metadata().await?.is_file() {
                match child.file_name().into_string() {
                    Ok(name) => {
                        found.push(name);
                    }
                    Err(_os_str) => log::warn!("{:?} is not a valid file name", _os_str),
                }
            }
        }
        Ok(found)
    }
}

#[derive(Debug)]
pub struct Context {
    option: ContextOption,
    pub short_version: &'static str,
}

impl std::ops::Deref for Context {
    type Target = ContextOption;
    fn deref(&self) -> &Self::Target {
        &self.option
    }
}
impl From<ContextOption> for Context {
    fn from(option: ContextOption) -> Self {
        let v = SHORT_VERSION.to_string() + "_" +  option.cpu.as_str();
        Self {
            option,
            short_version: Box::leak(v.into_boxed_str()),
        }
    }
}
#[inline(always)]
pub fn get() -> &'static Context {
    &CONTEXT
}
