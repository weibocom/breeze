// 定期更新discovery.
// 基于left-right实现无锁并发更新
//
use super::Discover;
use left_right::{Absorb, WriteHandle};
use std::io::{Error, ErrorKind, Result};
use std::path::PathBuf;
use std::time::Duration;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{interval, Interval};

use super::ServiceName;

unsafe impl<D, T> Send for AsyncServiceUpdate<D, T> where T: Absorb<(String, String)> {}
unsafe impl<D, T> Sync for AsyncServiceUpdate<D, T> where T: Absorb<(String, String)> {}

pub(crate) struct AsyncServiceUpdate<D, T>
where
    T: Absorb<(String, String)>,
{
    service: ServiceName,
    discovery: D,
    w: WriteHandle<T, (String, String)>,
    cfg: String,
    sign: String,
    interval: Interval,
    snapshot: PathBuf, // 本地快照存储的文件名称
}

impl<D, T> AsyncServiceUpdate<D, T>
where
    T: Absorb<(String, String)>,
{
    pub fn new(
        service: String,
        discovery: D,
        writer: WriteHandle<T, (String, String)>,
        tick: Duration,
        snapshot: String,
    ) -> Self {
        let snapshot = if snapshot.len() == 0 {
            "/tmp/breeze/services/snapshot".to_owned()
        } else {
            snapshot
        };
        let mut path = PathBuf::from(snapshot);
        path.push(&service);
        Self {
            service: ServiceName::from(service),
            discovery: discovery,
            w: writer,
            cfg: Default::default(),
            sign: Default::default(),
            interval: interval(tick),
            snapshot: path,
        }
    }
    fn path(&self) -> PathBuf {
        let mut pb = PathBuf::new();
        pb.push(&self.snapshot);
        pb.push(self.service.name());
        pb
    }
    // 如果当前配置为空，则从snapshot加载
    async fn load_from_snapshot(&mut self) -> Result<()>
    where
        D: Discover + Send + Unpin,
    {
        if self.cfg.len() != 0 {
            return Ok(());
        }
        let mut contents = vec![];
        File::open(&self.path())
            .await?
            .read_to_end(&mut contents)
            .await?;
        let mut contents = String::from_utf8(contents)
            .map_err(|_e| Error::new(ErrorKind::Other, "not a valid utfi file"))?;
        // 内容的第一行是签名，第二行是往后是配置
        let idx = contents.find('\n').unwrap_or(0);
        let cfg = contents.split_off(idx);
        self.cfg = cfg;
        self.sign = contents;

        self.w.append((self.cfg.clone(), self.service.to_owned()));
        self.w.publish();
        Ok(())
    }
    async fn dump_to_snapshot(&mut self) -> Result<()> {
        let path = self.snapshot.as_path();
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                tokio::fs::create_dir_all(parent).await?;
            }
        }
        let mut file = File::create(path).await?;
        file.write_all(self.sign.as_bytes()).await?;
        file.write_all(self.cfg.as_bytes()).await?;
        Ok(())
    }
    async fn load_from_discovery(&mut self) -> Result<()>
    where
        D: Discover + Send + Unpin,
    {
        use super::Config;
        match self
            .discovery
            .get_service(&self.service, &self.sign)
            .await?
        {
            Config::NotChanged => Ok(()),
            Config::NotFound => Err(Error::new(
                ErrorKind::NotFound,
                format!("service not found. name:{}", self.service.name()),
            )),
            Config::Config(cfg, sig) => {
                if self.cfg != cfg || self.sign != sig {
                    self.cfg = cfg;
                    self.sign = sig;
                    self.w.append((self.cfg.clone(), self.service.to_owned()));
                    self.w.publish();
                    if let Err(e) = self.dump_to_snapshot().await {
                        log::warn!(
                            "failed to dump to snapshot. path:{:?} {:?}",
                            self.snapshot,
                            e
                        );
                    };
                }
                Ok(())
            }
        }
    }
    pub async fn start_watch(&mut self)
    where
        D: Discover + Send + Unpin,
    {
        let _r = self.load_from_snapshot().await;
        if self.cfg.len() == 0 {
            let r = self.load_from_discovery().await;
            self.check(r);
        }
        loop {
            self.interval.tick().await;
            let r = self.load_from_discovery().await;
            self.check(r);
        }
    }
    fn check(&self, r: Result<()>) {
        match r {
            Ok(_) => {}
            Err(e) => {
                log::warn!(
                    "load service config error. service:{} error:{:?}",
                    self.service.name(),
                    e
                );
            }
        }
    }
}
