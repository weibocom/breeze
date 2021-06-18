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

unsafe impl<D, T> Send for AsyncServiceUpdate<D, T> where T: Absorb<String> {}
unsafe impl<D, T> Sync for AsyncServiceUpdate<D, T> where T: Absorb<String> {}

pub(crate) struct AsyncServiceUpdate<D, T>
where
    T: Absorb<String>,
{
    service: String,
    discovery: D,
    w: WriteHandle<T, String>,
    cfg: String,
    sign: String,
    interval: Interval,
    snapshot: String, // 本地快照存储的文件名称
}

impl<D, T> AsyncServiceUpdate<D, T>
where
    T: Absorb<String>,
{
    pub fn new(
        service: String,
        discovery: D,
        writer: WriteHandle<T, String>,
        tick: Duration,
        snapshot: String,
    ) -> Self {
        let snapshot = if snapshot.len() == 0 {
            "/tmp/breeze/discovery/services/snapshot".to_owned()
        } else {
            snapshot
        };
        Self {
            service: service,
            discovery: discovery,
            w: writer,
            cfg: Default::default(),
            sign: Default::default(),
            interval: interval(tick),
            snapshot: snapshot,
        }
    }
    fn path(&self) -> PathBuf {
        let mut pb = PathBuf::new();
        pb.push(&self.snapshot);
        pb.push(&self.service);
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

        self.w.append(self.cfg.clone());
        Ok(())
    }
    async fn dump_to_snapshot(&mut self) -> Result<()> {
        let mut file = File::create(&self.path()).await?;
        file.write_all(self.sign.as_bytes()).await?;
        file.write_all(self.cfg.as_bytes()).await?;
        Ok(())
    }
    async fn load_from_discovery(&mut self) -> Result<()>
    where
        D: Discover + Send + Unpin,
    {
        let (cfg, sign) = self
            .discovery
            .get_service(&self.service, &self.sign)
            .await?
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "no service config found"))?;
        if self.cfg != cfg {
            self.cfg = cfg;
            self.sign = sign;
            self.w.append(self.cfg.clone());
            self.dump_to_snapshot().await?;
        }
        Ok(())
    }
    pub async fn start_watch(&mut self)
    where
        D: Discover + Send + Unpin,
    {
        let r = self.load_from_snapshot().await;
        self.check(r);
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
                println!(
                    "load service config error. service:{} error:{:?}",
                    self.service, e
                );
            }
        }
    }
}
