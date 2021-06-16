use clap::{AppSettings, Clap};
use url::Url;
#[derive(Clap, Debug)]
#[clap(name = "resource mesh", version = "0.0.1", author = "IF")]
#[clap(setting = AppSettings::ColoredHelp)]
pub struct Context {
    #[clap(
        short,
        long,
        about("service registry url. e.g. vintage://127.0.0.1:8080")
    )]
    discovery: Url,
    #[clap(
        short,
        long,
        about("use unix or tcp socket for listener. (default: unix)"),
        default_value("unix")
    )]
    family: String,

    #[clap(short, long, about("groups for subscrbie"))]
    group: String,
}

impl Context {
    #[inline]
    pub fn from_os_args() -> Self {
        Context::parse()
    }
    pub fn listeners(&self) -> ListenerIter {
        ListenerIter { seq: 0 }
    }
    pub fn discovery(&self) -> Url {
        self.discovery.clone()
    }
    pub fn groups(&self) -> Vec<&str> {
        let groups: Vec<&str> = self.group.split(',').collect();
        groups
    }
    pub async fn wait(&self) {}
    pub fn check(&self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct Quadruple {}

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
        //"0.0.0.0:1234".to_owned()
    }
    pub fn protocol(&self) -> &'static str {
        "memcache"
    }
    pub fn service(&self) -> String {
        "feed.content".to_owned()
    }
    pub fn endpoint(&self) -> String {
        "cacheservice".to_owned()
        //"pipe".to_owned()
    }
}

pub struct ListenerIter {
    seq: usize,
}

impl Iterator for ListenerIter {
    type Item = Quadruple;
    fn next(&mut self) -> Option<Self::Item> {
        if self.seq >= 1 {
            None
        } else {
            self.seq += 1;
            Some(Quadruple::new())
        }
    }
}
