use clap::{AppSettings, Clap};
use std::io::{Error, ErrorKind, Result};
use std::path::Path;
use url::Url;

mod quadruple;
pub use quadruple::Quadruple;

#[derive(Clap, Debug)]
#[clap(name = "resource mesh", version = "0.0.1", author = "IF")]
#[clap(setting = AppSettings::ColoredHelp)]
pub struct Context {
    #[clap(long, about("port for suvervisor"), default_value("9984"))]
    port: u16,
    #[clap(
        short,
        long,
        about("service registry url. e.g. vintage://127.0.0.1:8080"),
        default_value("vintage://127.0.0.1:8080")
    )]
    discovery: Url,
    #[clap(
        short,
        long,
        about("path for saving snapshot of service topology."),
        default_value("/tmp/breeze/snapshot")
    )]
    snapshot: String,
    #[clap(
        short('p'),
        long,
        about("path for unix domain socket to listen."),
        default_value("/tmp/breeze/socks")
    )]
    service_path: String,
    #[clap(short, long, about("starting in upgrade mode"))]
    upgrade: bool,

    #[clap(short, long, about("log path"), default_value("/tmp/breeze/logs"))]
    log_dir: String,

    #[clap(short, long, about("metrics url"))]
    metrics_url: Option<String>,

    #[clap(
        long,
        about("establish a connection to select an local ip"),
        default_value("10.10.10.10:53")
    )]
    pub metrics_probe: String,
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
        Ok(())
    }
    pub fn log_dir(&self) -> &str {
        &self.log_dir
    }
    // 如果是以升级模式启动，则会将原有的端口先关闭。
    pub fn listeners(&self) -> ListenerIter {
        ListenerIter {
            path: self.service_path.to_string(),
            listened: Default::default(),
            snapshot: self.snapshot.to_string(),
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
}

use std::collections::HashMap;
pub struct ListenerIter {
    listened: HashMap<String, ()>,
    path: String,
    snapshot: String,
}

impl ListenerIter {
    // 扫描self.pah，获取该目录下所有不以.sock结尾，符合格式的文件作为服务配置进行解析。
    // 不以.sock结尾，由'@'字符分隔成一个Quard的配置。一个标准的服务配置文件名为
    // 如果对应的文件已经存在 $name.sock。那说明有其他进程侦听了该服务
    pub async fn scan(&mut self) -> Result<Vec<Quadruple>> {
        let mut listeners = vec![];
        for name in self.read_all().await?.iter() {
            if self.listened.contains_key(name) {
                continue;
            }
            if let Some(one) = Quadruple::parse(name, &self.snapshot) {
                listeners.push(one);
                self.listened.insert(name.to_string(), ());
            }
        }
        Ok(listeners)
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
