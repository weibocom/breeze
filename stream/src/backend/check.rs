use crate::{BackendStream, MpmcStream};
use ds::{Cid, Ids};
use protocol::{Protocol, Resource};

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{interval_at, sleep, timeout, Interval};

pub struct BackendBuilder {
    finished: Arc<AtomicBool>,
    inited: Arc<AtomicBool>,
    stream: Arc<MpmcStream>,
    ids: Arc<Ids>,
}

impl BackendBuilder {
    pub fn from<P>(parser: P, addr: &str, parallel: usize, rsrc: Resource, biz: &str) -> Self
    where
        P: Unpin + Send + Sync + Protocol + 'static + Clone,
    {
        let finished = Arc::new(AtomicBool::new(false));
        let init = Arc::new(AtomicBool::new(false));
        let stream = Arc::new(MpmcStream::with_capacity(parallel, biz, addr, rsrc));
        let checker = BackendChecker::from(stream.clone(), finished.clone(), init.clone(), parser);
        rt::spawn(async { checker.start_check().await });

        Self {
            stream: stream,
            ids: Arc::new(Ids::with_capacity(parallel)),
            finished: finished,
            inited: init,
        }
    }
    pub fn build(&self, fake_cid: bool) -> BackendStream {
        let cid = if fake_cid {
            Cid::fake()
        } else {
            self.ids
                .next()
                .map(|cid| Cid::new(cid, self.ids.clone()))
                .unwrap_or_else(|| {
                    log::info!("connection id overflow. {}", self.stream.address());
                    Cid::fake()
                })
        };
        BackendStream::from(cid, self.stream.clone())
    }
    // 已经连接上或者至少连接了一次
    #[inline]
    pub fn inited(&self) -> bool {
        self.inited.load(Ordering::Acquire)
    }
}

impl Drop for BackendBuilder {
    fn drop(&mut self) {
        self.finished.store(true, Ordering::Release);
    }
}

pub struct BackendChecker<P> {
    inner: Arc<MpmcStream>,
    rx: Receiver<u8>,
    tx: Arc<Sender<u8>>,
    connecting: bool, // 当前是否需要重新建立连接
    finished: Arc<AtomicBool>,
    inited: Arc<AtomicBool>,
    instant_conn: Instant,    // 最近一次连接开始时间
    instant_timeout: Instant, // 最近一次开始计算timeout的时间
    tries: usize,             // 最近一次连接失败的次数
    tick: Interval,
    req_num: usize, // 检查timeout时，上一次的请求号。
    parser: P,
}

impl<P> BackendChecker<P> {
    fn from(
        stream: Arc<MpmcStream>,
        finished: Arc<AtomicBool>,
        inited: Arc<AtomicBool>,
        parser: P,
    ) -> Self {
        let (tx, rx) = channel(8);
        // 用一个random的耗时，将不同的backend的tick错开。
        const SECS: u64 = 2;
        let latency_ms: u64 = rand::random::<u64>() % (SECS * 1000);
        let start = Instant::now() + Duration::from_millis(latency_ms);
        let tick = interval_at(start.into(), Duration::from_secs(SECS));
        let (req_num, _) = stream.load_ping_pong();
        Self {
            tx: Arc::new(tx),
            rx: rx,
            inner: stream,
            req_num: req_num,
            tries: 0,
            connecting: true, // 初始化时需要建立连接
            instant_conn: Instant::now(),
            instant_timeout: Instant::now(),
            tick: tick,
            finished: finished,
            inited: inited,
            parser: parser,
        }
    }
    async fn start_check(mut self)
    where
        P: Protocol,
    {
        let noop = futures_task::noop_waker();
        let mut ctx = Context::from_waker(&noop);
        while !self.finished.load(Ordering::Acquire) {
            match self.rx.poll_recv(&mut ctx) {
                Poll::Ready(Some(_)) => {
                    self.connecting = true;
                }
                _ => {}
            }
            if self.connecting {
                self.try_connect().await;
            }
            self.check_timeout();
            self.tick.tick().await;
        }
        log::info!("finished {}. closing and shutting down.", self.address());
        self.try_close();
        sleep(Duration::from_secs(15)).await;
        self.shutdown_all();
    }
    async fn try_connect(&mut self)
    where
        P: Protocol,
    {
        let expected = Duration::from_secs(1 << ((1 + self.tries).min(8)) as u64);
        if self.tries == 0 || self.instant_conn.elapsed() >= expected {
            self.instant_conn = Instant::now();
            log::debug!("try to connect {} tries:{}", self.address(), self.tries);
            match self.reconnected_once().await {
                Ok(_) => {
                    self.tries = 0;
                    self.connecting = false;
                }
                Err(e) => {
                    log::warn!(
                        "{}-th connecting to {} err:{}",
                        self.tries,
                        self.address(),
                        e
                    );
                    metrics::status("status", metrics::Status::Down, self.metric_id());
                }
            };
        }
        self.tries += 1;
        self.inited.store(true, Ordering::Release);
    }
    async fn reconnected_once(&self) -> std::result::Result<(), Box<dyn std::error::Error>>
    where
        P: Unpin + Send + Sync + Protocol + 'static + Clone,
    {
        let addr = self.address();
        let stream = timeout(Duration::from_secs(2), TcpStream::connect(addr)).await??;
        let _ = stream.set_nodelay(true);
        log::debug!("connected to {}", addr);
        let (r, w) = stream.into_split();
        let req_stream = self.inner.clone();

        req_stream.bridge(
            self.parser.clone(),
            r,
            w,
            Notifier {
                tx: self.tx.clone(),
            },
        );
        Ok(())
    }

    // 满足以下所有条件，则把done调整为true，当前实例快速失败。
    // 1. req_num停止更新；
    // 2. resp_num > req_num;
    // 3. 超过7秒钟。why 7 secs?
    fn check_timeout(&mut self) {
        const TIME_OUT: Duration = Duration::from_secs(4);
        // 已经done了，忽略
        let done = self.inner.done();
        let (req_num, resp_num) = self.inner.load_ping_pong();
        if done || req_num != self.req_num || resp_num == req_num {
            self.req_num = req_num;
            self.instant_timeout = Instant::now();
            return;
        }
        // 判断是否超时
        let elap = self.instant_timeout.elapsed();
        if elap <= TIME_OUT {
            // 还未超时
            return;
        }
        log::error!(
            "timeout: no response return in {:?}. stream marked done. req:{} resp:{}, addr:{}",
            elap,
            req_num,
            resp_num,
            self.address()
        );
        self.inner.mark_done();
        metrics::status("status", metrics::Status::Timeout, self.metric_id());
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

impl<P> std::ops::Deref for BackendChecker<P> {
    type Target = MpmcStream;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
