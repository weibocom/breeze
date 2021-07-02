use clap::{AppSettings, Clap};
use std::io::{Error, ErrorKind, Result};
use std::path::Path;
use std::time::Duration;
use url::Url;

#[derive(Clap, Debug)]
#[clap(name = "resource mesh", version = "0.0.1", author = "IF")]
#[clap(setting = AppSettings::ColoredHelp)]
pub struct Context {
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
}

impl Context {
    #[inline]
    pub fn from_os_args() -> Self {
        Context::parse()
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
    orig_addr: Option<String>,
}

// feed.content#yf@unix@memcache@cacheservice
impl Quadruple {
    fn from(
        name: String,
        service: String,
        family: String,
        protocol: String,
        endpoint: String,
        snapshot: String,
        addr: String,
        orig_addr: Option<String>,
    ) -> Self {
        Self {
            name,
            service,
            family,
            protocol,
            endpoint,

            snapshot,
            addr,
            orig_addr,
        }
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

            let mut orig = None;
            let mut sock = self.sock_name(name);
            // 对应的sock已经存在，但又不是升级。
            if self.is_sock_exists(&sock) {
                if !self.upgrade {
                    log::warn!("{} listened by other process, but current process is not in upgrade mode. ", sock);
                    continue;
                }
                // 升级模式。
                orig = Some(sock);
                sock = self.sock_name_upgrade(&name);
            }
            if let Some((service, protocol, backend)) = self.parse(name) {
                let one = Quadruple::from(
                    name.to_owned(),
                    service,
                    "unix".to_owned(),
                    protocol,
                    backend,
                    self.snapshot.to_string(),
                    sock,
                    orig,
                );
                listeners.push(one);
            } else {
                log::warn!(
                    "{} is not a valid service or processed by other processor",
                    name
                );
                continue;
            }
        }
        Ok(listeners)
    }
    // service@protocol@backend_type
    // service: 服务名称
    // protocol: 处理client连接的协议。memcache、redis等支持的协议
    // backend_type: 后端资源的类型。是cacheservice、redis_dns等等
    fn parse(&self, name: &str) -> Option<(String, String, String)> {
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
        Some((
            fields[0].to_string(),
            fields[1].to_string(),
            fields[2].to_string(),
        ))
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
                        if name.len() < 100 {
                            found.push(name)
                        } else {
                            log::warn!(
                                "{} is not a valid file name. len {} is greater than 100",
                                name,
                                name.len()
                            );
                        }
                    }
                    Err(os_str) => log::warn!("{:?} is not a valid file name", os_str),
                }
            }
        }
        Ok(found)
    }
    pub async fn on_listened(&mut self, qt: Quadruple) -> Result<()> {
        // TODO
        // 1. 可能会有并发问题
        // 2. 可能listen会失败，但listened已经标识成功，导致遗漏
        // 如果当前orig不是None，说明是升级模式。要把old move走，当前的mv成老的。
        if let Some(orig) = qt.orig_addr {
            // 删除老的，upgrading move到正常的sock
            tokio::fs::remove_file(&orig).await?;
            tokio::fs::rename(qt.addr, orig).await?;
        }
        self.listened.insert(qt.name, ());
        Ok(())
    }
}
