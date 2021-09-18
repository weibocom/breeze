use crate::{BackendStream, RingBufferStream};
use ds::{Cid, Ids};
use protocol::{Protocol, Resource};

use std::io::Result;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::sleep;

pub struct BackendBuilder {
    checker: Arc<BackendChecker>,
    ids: Arc<Ids>,
}

impl BackendBuilder {
    pub fn from<P>(parser: P, addr: &str, parallel: usize, rsrc: Resource, biz: &str) -> Self
    where
        P: Unpin + Send + Sync + Protocol + 'static + Clone,
    {
        let stream = Arc::new(RingBufferStream::with_capacity(parallel, biz, addr, rsrc));
        let (tx, rx) = channel(8);
        let checker = Arc::new(BackendChecker::from(stream.clone(), tx));
        checker.clone().start_check(parser.clone(), rx);
        checker.clone().start_check_timeout();
        let me = Self {
            checker: checker,
            ids: Arc::new(Ids::with_capacity(parallel)),
        };
        me
    }
    pub fn build(&self) -> BackendStream {
        self.ids
            .next()
            .map(|cid| {
                BackendStream::from(Cid::new(cid, self.ids.clone()), self.checker.inner.clone())
            })
            .unwrap_or_else(|| {
                log::info!("connection id overflow. {}", self.checker.addr());
                BackendStream::not_connected()
            })
    }
    // 已经连接上或者至少连接了一次
    #[inline]
    pub fn inited(&self) -> bool {
        !self.checker.done() || self.checker.num.load(Ordering::Acquire) >= 1
    }
}

impl Drop for BackendBuilder {
    fn drop(&mut self) {
        self.checker.finished.store(true, Ordering::Release);
        // 结束流处理。
        self.checker.try_close();
    }
}

pub struct BackendChecker {
    inner: Arc<RingBufferStream>,
    tx: Arc<Sender<u8>>,
    finished: AtomicBool,
    num: AtomicUsize, // 建立连接的数量。
}

impl BackendChecker {
    fn from(stream: Arc<RingBufferStream>, tx: Sender<u8>) -> Self {
        if let Err(e) = tx.try_send(0) {
            log::error!("failed to send connect signal to {}:{:?}", stream.addr(), e);
        }
        let me = Self {
            tx: Arc::new(tx),
            inner: stream,
            finished: AtomicBool::new(false),
            num: AtomicUsize::new(0),
        };
        me
    }

    // 在mpmc::RingBufferStream的done变为true时，会向rx发送一个信号。
    fn start_check<P>(self: Arc<Self>, parser: P, mut rx: Receiver<u8>)
    where
        P: Unpin + Send + Sync + Protocol + 'static + Clone,
    {
        tokio::spawn(async move {
            while let Some(_) = rx.recv().await {
                if self.finished.load(Ordering::Acquire) {
                    break;
                }
                log::debug!("signal recived, try to connect:{}", self.inner.addr());
                self.connect(parser.clone()).await;
            }
            log::info!("check done complete:{}", self.inner.addr());
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
                    log::debug!("check: {} connected tries:{}", self.addr(), tries);
                    break;
                }
                Err(e) => {
                    log::info!("connect {} failed:{:?} tries:{}", self.addr(), e, tries);
                    let secs = 1 << tries.min(5);
                    tries += 1;
                    self.num.fetch_add(1, Ordering::AcqRel);
                    metrics::status("status", metrics::Status::Down, self.metric_id());
                    sleep(Duration::from_secs(secs)).await;
                }
            }
        }
    }

    async fn check_reconnected_once<P>(&self, parser: P) -> Result<()>
    where
        P: Unpin + Send + Sync + Protocol + 'static + Clone,
    {
        let addr = self.addr();
        let stream =
            tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(addr)).await??;
        log::debug!("connected to {}", addr);
        let _ = stream.set_nodelay(true);
        let (r, w) = stream.into_split();
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
        log::debug!("check: {} timeout task started", self.addr());
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
                self.addr()
            );
            self.inner.mark_done();
            metrics::status("status", metrics::Status::Timeout, self.metric_id());
        }
        log::info!("timeout task checker complete:{}", self.addr());
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

impl std::ops::Deref for BackendChecker {
    type Target = RingBufferStream;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
