use clap::{AppSettings, Clap};
use std::io::{Error, ErrorKind, Result};
use std::path::Path;
use std::time::Duration;
use url::Url;

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
        default_value("weibo.com:80")
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
            upgrade: self.upgrade,
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

#[derive(Debug, Clone)]
pub struct Quadruple {
    name: String,
    service: String,
    family: String,
    protocol: String,
    endpoint: String,
    snapshot: String,
    addr: String,
}

// feed.content#yf@unix@memcache@cacheservice
impl Quadruple {
    // service@protocol@backend_type
    // service: 服务名称
    // protocol: 处理client连接的协议。memcache、redis等支持的协议.  格式为 mc:port.
    // backend_type: 后端资源的类型。是cacheservice、redis_dns等等
    fn parse(name: &str, snapshot: &str) -> Option<Self> {
        let name = Path::new(name)
            .file_name()
            .map(|s| s.to_str())
            .unwrap_or(None)
            .unwrap_or("");
        let fields: Vec<&str> = name.split('@').collect();
        if fields.len() != 3 {
            log::warn!(
                "not a valid service file name:{}. must contains 4 fields seperated by '@'",
                name
            );
            return None;
        }
        let service = fields[0];
        let protocol_item = fields[1];
        let protocol_fields: Vec<&str> = protocol_item.split(':').collect();
        if protocol_fields.len() != 2 {
            log::warn!(
                "not a valid service file name::{} protocol {} must be splited by ':'",
                name,
                protocol_item
            );
            return None;
        }
        let protocol = protocol_fields[0];
        let family = "tcp";
        if let Err(e) = protocol_fields[1].parse::<u16>() {
            log::warn!(
                "not a valid service file name:{} not a valid port:{} error:{:?}",
                name,
                protocol_fields[1],
                e
            );
            return None;
        }
        let addr = "127.0.0.1:".to_string() + protocol_fields[1];

        let backend = fields[2];
        Some(Self {
            name: name.to_owned(),
            service: service.to_owned(),
            family: family.to_string(),
            protocol: protocol.to_owned(),
            endpoint: backend.to_string(),
            addr: addr.to_string(),
            snapshot: snapshot.to_string(),
        })
    }
    pub fn name(&self) -> String {
        self.name.to_owned()
    }
    pub fn family(&self) -> String {
        self.family.to_owned()
    }
    pub fn address(&self) -> String {
        self.addr.to_owned()
    }
    pub fn protocol(&self) -> String {
        self.protocol.to_owned()
    }
    pub fn service(&self) -> String {
        self.service.to_owned()
    }
    pub fn endpoint(&self) -> String {
        self.endpoint.to_owned()
    }
    pub fn snapshot(&self) -> String {
        self.snapshot.to_owned()
    }
    pub fn tick(&self) -> Duration {
        Duration::from_secs(6)
    }
}

use std::collections::HashMap;
pub struct ListenerIter {
    upgrade: bool,
    listened: HashMap<String, ()>,
    path: String,
    snapshot: String,
}

const SOCK_APPENDIX: &str = ".sock";

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
            // 如果是sock文件，说明有其他进程listen了该服务。
            if self.is_sock(name) {
                continue;
            }

            if let Some(one) = Quadruple::parse(name, &self.snapshot) {
                listeners.push(one);
                self.listened.insert(name.to_string(), ());
            }
        }
        Ok(listeners)
    }
    // 升级中的名称。
    fn sock_name_upgrade(&self, name: &str) -> String {
        let mut s = self.sock_name(name);
        s.push_str(".u");
        s
    }
    fn sock_name(&self, name: &str) -> String {
        let mut s: String = name.to_owned();
        s.push_str(SOCK_APPENDIX);
        s
    }
    fn is_sock(&self, path: &str) -> bool {
        Path::new(path).extension().map(|ext| ext.to_str()) == Some(Some(SOCK_APPENDIX))
    }
    fn is_sock_exists(&self, sock: &str) -> bool {
        Path::new(sock).exists()
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
