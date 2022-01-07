use clap::Parser;
use std::io::{Error, ErrorKind, Result};
use std::path::Path;
use url::Url;

mod quadruple;
pub use quadruple::Quadruple;

#[derive(Parser)]
#[clap(
    name = "resource mesh",
    version = "BREEZE_VERSION_PLACE_HOLDER",
    author = "IF"
)]
pub struct Context {
    #[clap(long, help("port for suvervisor"), default_value("9984"))]
    port: u16,

    #[clap(
        short,
        long,
        help("service registry url. e.g. vintage://127.0.0.1:8080"),
        default_value("vintage://127.0.0.1:8080")
    )]
    discovery: Url,

    #[clap(
        short,
        long,
        help("idc config path"),
        default_value("/3/config/breeze/idc_region")
    )]
    idc_path: String,

    #[clap(
        short,
        long,
        help("interval of updating config (unit second)"),
        default_value("15")
    )]
    tick_sec: usize,

    #[clap(
        short,
        long,
        help("path for saving snapshot of service topology."),
        default_value("/tmp/breeze/snapshot")
    )]
    snapshot: String,
    #[clap(
        short('p'),
        long,
        help("path for unix domain socket to listen."),
        default_value("/tmp/breeze/socks")
    )]
    service_path: String,
    #[clap(short, long, help("starting in upgrade mode"))]
    upgrade: bool,

    #[clap(short, long, help("log path"), default_value("/tmp/breeze/logs"))]
    log_dir: String,

    #[clap(short, long, help("metrics url"))]
    metrics_url: Option<String>,

    #[clap(
        long,
        help("establish a connection to select an local ip"),
        default_value("10.10.10.10:53")
    )]
    pub metrics_probe: String,

    #[clap(long, help("log level. debug|info|warn|error"), default_value("info"))]
    pub log_level: String,
}

impl Context {
    #[inline]
    pub fn from_os_args() -> Self {
        Context::parse()
    }
    pub fn port(&self) -> u16 {
        self.port
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
    pub fn log_dir(&self) -> &str {
        &self.log_dir
    }
    // 如果是以升级模式启动，则会将原有的端口先关闭。
    pub fn listeners(&self) -> ListenerIter {
        ListenerIter {
            path: self.service_path.to_string(),
            processed: Default::default(),
        }
    }
    pub fn discovery(&self) -> Url {
        self.discovery.clone()
    }
    pub fn service_path(&self) -> String {
        self.service_path.clone()
    }
    pub fn metrics_url(&self) -> String {
        self.metrics_url.clone().unwrap_or_default()
    }
    pub fn snapshot(&self) -> &str {
        &self.snapshot
    }
    pub fn idc_path(&self) -> String {
        self.idc_path.clone()
    }
}

use std::collections::HashMap;
pub struct ListenerIter {
    processed: HashMap<String, ()>,
    path: String,
}

impl ListenerIter {
    // 扫描self.pah，获取该目录下所有不以.sock结尾，符合格式的文件作为服务配置进行解析。
    // 不以.sock结尾，由'@'字符分隔成一个Quard的配置。一个标准的服务配置文件名为
    // 如果对应的文件已经存在 $name.sock。那说明有其他进程侦听了该服务
    pub async fn scan(&mut self) -> Vec<Quadruple> {
        let mut listeners = vec![];
        match self.read_all().await {
            Ok(names) => {
                for name in names {
                    if self.processed.contains_key(&name) {
                        continue;
                    }
                    if let Some(one) = Quadruple::parse(&name) {
                        log::info!("service parsed :{}", one);
                        listeners.push(one);
                        self.processed.insert(name.to_string(), ());
                    }
                }
            }
            Err(e) => log::warn!("failed to scan '{}' err:{:?}", self.path, e),
        }
        listeners
    }

    pub fn on_fail(&mut self, name: String) {
        let s = self.path.clone() + "/" + &name;
        if self.processed.contains_key(&s) {
            self.processed.remove(&s);
            log::warn!("listenerIter on_fail exist:{}", s);
        } else {
            log::warn!("listenerIter on_fail :{}", s);
        }
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
                    Err(os_str) => log::warn!("{:?} is not a valid file name", os_str),
                }
            }
        }
        Ok(found)
    }
}
