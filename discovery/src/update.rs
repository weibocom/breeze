// 定期更新discovery.
// 基于left-right实现无锁并发更新
//
use super::Discover;
use futures::ready;
use left_right::{Absorb, WriteHandle};
use std::time::Duration;
use tokio::time::{interval, Interval};

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
    // 如果当前配置为空，则从snapshot加载
    fn load_from_snapshot(&mut self)
    where
        D: Discover + Send + Unpin,
    {
        if self.cfg.len() == 0 {}
    }
    fn load_from_discovery(&mut self) {}
}

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
impl<D, T> Future for AsyncServiceUpdate<D, T>
where
    D: Discover + Send + Unpin,
    T: Absorb<String>,
{
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = &mut *self;
        me.load_from_snapshot();
        if me.cfg.len() == 0 {
            me.load_from_discovery();
        }
        loop {
            ready!(me.interval.poll_tick(cx));
            me.load_from_discovery();
        }
    }
}
use std::io::Result;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

pub(super) async fn load_service_snapshot(service: &str, snapshot: &str) -> (String, String) {
    match _load_service(service, snapshot).await {
        Ok(s) => s,
        Err(_e) => ("".to_string(), "".to_string()),
    }
}
async fn _load_service(service: &str, snapshot: &str) -> Result<(String, String)> {
    let path = format!("{}/{}", snapshot, service);
    let mut file = File::open(&path).await?;

    let mut contents = vec![];
    file.read_to_end(&mut contents).await?;

    let contents = String::from_utf8(contents).unwrap_or_else(|e| {
        println!(
            "snapshot loaded {}, but is not valid utf8 format:{:?}",
            &path, e
        );
        "".to_string()
    });
    Ok((contents, "".to_string()))
}
pub(super) async fn dump_service_snapshot(service: &str, cfg: &str, sign: &str, snapshot: &str) {}
