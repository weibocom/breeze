use crate::{BackendStream, RingBufferStream};
use ds::{Cid, Ids};
use protocol::{Protocol, Resource};

use std::io::Result;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::sleep;

pub struct BackendBuilder {
    addr: String,
    finished: Arc<AtomicBool>,
    stream: Arc<RingBufferStream>,
    ids: Arc<Ids>,
}

impl BackendBuilder {
    pub fn from<P>(parser: P, addr: &str, parallel: usize, rsrc: Resource, biz: &str) -> Self
    where
        P: Unpin + Send + Sync + Protocol + 'static + Clone,
    {
        let stream = Arc::new(RingBufferStream::with_capacity(parallel));
        let me = Self {
            addr: addr.to_string(),
            finished: Arc::new(AtomicBool::new(false)),
            stream: stream.clone(),
            ids: Arc::new(Ids::with_capacity(parallel)),
        };
        let (tx, rx) = channel(8);
        let checker = Arc::new(BackendChecker::from(
            stream.clone(),
            addr.to_string(),
            me.finished.clone(),
            tx,
            rsrc,
            biz,
        ));
        checker.clone().start_check_done(parser.clone(), rx);
        checker.start_check_timeout();
        me
    }
    pub fn build(&self) -> BackendStream {
        self.ids
            .next()
            .map(|cid| {
                BackendStream::from(
                    Cid::new(cid, self.ids.clone()),
                    self.addr.clone(),
                    self.stream.clone(),
                )
            })
            .unwrap_or_else(|| {
                log::info!("connection id overflow, connection established failed");
                BackendStream::not_connected()
            })
    }
    fn finish(&self) {
        self.finished.store(true, Ordering::Release);
    }
}

impl Drop for BackendBuilder {
    fn drop(&mut self) {
        log::info!("{} finished. stream will be closed later", self.addr);
        self.finish();
    }
}

pub struct BackendChecker {
    inner: Arc<RingBufferStream>,
    tx: Arc<Sender<u8>>,
    addr: String,
    finished: Arc<AtomicBool>,
    resource: Resource,
    biz: String,
}

impl BackendChecker {
    fn from(
        stream: Arc<RingBufferStream>,
        addr: String,
        finished: Arc<AtomicBool>,
        tx: Sender<u8>,
        resource: Resource,
        biz: &str,
    ) -> Self {
        if let Err(e) = tx.try_send(0) {
            log::error!("check: failed to send connect signal to {}:{:?}", addr, e);
        }
        let me = Self {
            tx: Arc::new(tx),
            inner: stream,
            addr: addr,
            finished: finished,
            resource: resource,
            biz: biz.to_string(),
        };
        me
    }

    fn start_check_done<P>(self: Arc<Self>, parser: P, mut rx: Receiver<u8>)
    where
        P: Unpin + Send + Sync + Protocol + 'static + Clone,
    {
        tokio::spawn(async move {
            while let Some(_) = rx.recv().await {
                log::info!(
                    "check: connect signal recived, try to connect:{}",
                    self.addr
                );
                self.connect(parser.clone()).await;
            }
            log::info!("check: stream complete:{}", self.addr);
        });
    }

    async fn connect<P>(&self, parser: P)
    where
        P: Unpin + Send + Sync + Protocol + 'static + Clone,
    {
        let mut tries = 0;
        while !self.finished.load(Ordering::Acquire) {
            match self.check_reconnected_once(parser.clone()).await {
                Ok(_) => {
                    log::debug!("check: {} connected tries:{}", self.addr, tries);
                    break;
                }
                Err(e) => {
                    log::info!(
                        "check: {} connected failed:{:?} tries:{}",
                        self.addr,
                        e,
                        tries
                    );
                    let secs = (1 << tries).min(31);
                    tries += 1;
                    sleep(Duration::from_secs(secs)).await;
                }
            }
        }
    }

    async fn check_reconnected_once<P>(&self, parser: P) -> Result<()>
    where
        P: Unpin + Send + Sync + Protocol + 'static + Clone,
    {
        let addr = &self.addr;
        let stream =
            tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(addr)).await??;
        log::info!("check: connected to {}", addr);
        let _ = stream.set_nodelay(true);
        let (r, w) = stream.into_split();
        let r = super::Reader::from(r, addr, self.resource, &self.biz);
        let w = super::Writer::from(w, addr, self.resource, &self.biz);
        let req_stream = self.inner.clone();

        req_stream.bridge(
            parser.clone(),
            r,
            w,
            Notifier {
                tx: self.tx.clone(),
            },
        );
        Ok(())
    }
    fn start_check_timeout(self: Arc<Self>) {
        log::info!("check: {} timeout task started", self.addr);
        tokio::spawn(async move {
            self._start_check_timeout().await;
        });
    }

    // 满足以下所有条件，则把done调整为true，当前实例快速失败。
    // 1. req_num停止更新；
    // 2. resp_num > req_num;
    // 3. 超过7秒钟。why 7 secs?
    async fn _start_check_timeout(self: Arc<Self>) {
        use std::time::Instant;
        const TIME_OUT: Duration = Duration::from_secs(4);

        let (mut last_req, _) = self.inner.load_ping_ping();
        let mut duration = Instant::now();
        while !self.finished.load(Ordering::Acquire) {
            sleep(Duration::from_secs(1)).await;
            // 已经done了，忽略
            let done = self.inner.done();
            let (req_num, resp_num) = self.inner.load_ping_ping();
            if done || req_num != last_req || resp_num == req_num {
                last_req = req_num;
                duration = Instant::now();
                continue;
            }
            // 判断是否超时
            let elap = duration.elapsed();
            if elap <= TIME_OUT {
                // 还未超时
                continue;
            }
            log::error!(
                "check-timeout: no response return in {:?}. stream marked done. req:{} resp:{}, addr:{}",
                elap,
                req_num,
                resp_num,
                self.addr
            );
            self.inner.mark_done();
        }
    }
}

impl crate::Notify for Notifier {
    fn notify(&self) {
        if let Err(e) = self.tx.try_send(0) {
            log::error!("notify: failed to send notify signal:{:?}", e);
        }
    }
}

#[derive(Clone)]
pub struct Notifier {
    tx: Arc<Sender<u8>>,
}
