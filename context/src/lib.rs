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
        default_value("/tmp/breeze/services/snapshot")
    )]
    snapshot: String,
    #[clap(
        short('p'),
        long,
        about("path for unix domain socket to listen."),
        default_value("/tmp/breeze/services/socks")
    )]
    service_path: String,
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
    pub fn listeners(&self) -> ListenerIter {
        ListenerIter {
            path: self.service_path.to_string(),
            listened: Default::default(),
        }
    }
    pub fn discovery(&self) -> Url {
        self.discovery.clone()
    }
    pub fn service_path(&self) -> String {
        self.service_path.clone()
    }
}

#[derive(Debug)]
pub struct Quadruple {}

// feed.content#yf@unix@memcache@cacheservice
impl Quadruple {
    fn new() -> Self {
        Self {}
    }
    pub fn family(&self) -> String {
        "unix".to_owned()
        //"tcp".to_owned()
    }
    pub fn address(&self) -> String {
        "/tmp/sock/feed.content.sock".to_owned()
    }
    //pub fn protocol(&self) -> &'static str {
    //    "memcache"
    //}
    pub fn service(&self) -> String {
        "feed.content".to_owned()
    }
    pub fn endpoint(&self) -> String {
        "cacheservice".to_owned()
        //"pipe".to_owned()
    }
    pub fn snapshot(&self) -> String {
        "/tmp/breeze/services/snapshot".to_string()
    }
    pub fn tick(&self) -> Duration {
        Duration::from_secs(6)
    }
}

use std::collections::HashMap;
pub struct ListenerIter {
    listened: HashMap<String, ()>,
    path: String,
}

impl ListenerIter {
    pub async fn next(&mut self) -> Option<Quadruple> {
        None
    }
    //    async fn read_all(&self) -> Result<Vec<String>> {}
}
